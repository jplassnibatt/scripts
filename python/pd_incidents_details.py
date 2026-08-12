#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, List, Optional
import requests

__version__ = "1.4.3"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_lookback_span(span_str: str) -> timedelta:
    """Parses dynamic lookback strings (e.g., '2d', '3w', '1m', '1y') into a timedelta[cite: 18]."""
    match = re.match(r"^(\d+)([dwmy])$", span_str.strip().lower())
    if not match:
        raise ValueError(
            f"Invalid time span format: '{span_str}'. Use format like '2d', '3w', '1m', or '1y'."
        )

    amount = int(match.group(1))
    unit = match.group(2)

    if unit == "d":
        return timedelta(days=amount)
    elif unit == "w":
        return timedelta(weeks=amount)
    elif unit == "m":
        return timedelta(days=amount * 30)
    elif unit == "y":
        return timedelta(days=amount * 365)

    raise ValueError(f"Unsupported unit: '{unit}'")


def parse_timezone(tz_str: str) -> tzinfo:
    """Parses timezone strings into tzinfo objects (supports UTC, offsets like +05:00/-08:00, or IANA names)[cite: 18]."""
    tz_str = tz_str.strip()
    if tz_str.upper() in ("UTC", "Z"):
        return timezone.utc

    offset_match = re.match(r"^([+-])(\d{2}):?(\d{2})$", tz_str)
    if offset_match:
        sign, hours, minutes = offset_match.groups()
        total_minutes = int(hours) * 60 + int(minutes)
        if sign == "-":
            total_minutes = -total_minutes
        return timezone(timedelta(minutes=total_minutes))

    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(tz_str)
    except Exception:
        logger.error(
            f"Invalid timezone identifier: '{tz_str}'. Use IANA format (e.g., 'America/Santiago')."
        )
        sys.exit(1)


class PagerDutyAPI:
    """PagerDuty REST API v2 client with built-in rate-limiting and session management[cite: 18].

    Handles default rate limits of $Rate = 250\text{ req/min}$ with client-side throttling[cite: 18, 19].
    """

    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token or api_token.strip() == "":
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.min_interval = 1.0 / rate_limit
        self.last_request = 0.0

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-IncidentExporter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces client-side rate limiting ($Rate = 8\text{ req/s}$)[cite: 18]."""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()

    def _request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes API request with exponential backoff and rate-limit mitigation[cite: 18, 19]."""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication/Authorization failed (HTTP {response.status_code}). Check token permissions."
                    )
                    sys.exit(1)

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Timeout encountered. Retrying ({attempt + 1}/{max_retries})..."
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error("Request timed out after maximum retries.")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Request failed ({e}). Retrying ({attempt + 1}/{max_retries})..."
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Request failed permanently: {e}")
                    return None

        return None

    def validate_token(self) -> bool:
        """Validates API token credentials against the `/users` endpoint[cite: 18]."""
        logger.info("Validating API token...")
        response = self._request(f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def get_incidents(
        self, since: Optional[str] = None, until: Optional[str] = None, time_zone: str = "UTC"
    ) -> List[Dict]:
        """Fetches all incidents within specified date range natively evaluated by PagerDuty's time_zone handler[cite: 18]."""
        incidents = []
        offset = 0
        limit = 100

        logger.info(f"Fetching incidents from native API window: {since or 'Beginning'} -> {until or 'Now'} (TZ: {time_zone})")

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "include[]": ["first_trigger_log_entry"],
                "time_zone": time_zone,
            }
            if since:
                params["since"] = since
            if until:
                params["until"] = until

            response = self._request(f"{self.base_url}/incidents", params=params)
            if not response:
                logger.error("Failed to fetch incident batch.")
                break

            data = response.json()
            batch = data.get("incidents", [])
            incidents.extend(batch)

            if data.get("more", False):
                logger.info(f"Fetched {len(incidents)} incidents so far...")
                offset += limit
            else:
                logger.info(f"✓ Completed retrieval: {len(incidents)} total incidents")
                break

        return incidents


def extract_incident_data(
    incidents: List[Dict], target_tz: Optional[tzinfo] = None
) -> List[Dict]:
    """Extracts and flattens incident records directly from natively localized API responses[cite: 18]."""
    results = []

    for incident in incidents:
        trigger_summary = "N/A"
        if incident.get("first_trigger_log_entry"):
            trigger_summary = incident["first_trigger_log_entry"].get("summary", "N/A")

        created_at_raw = incident.get("created_at", "N/A")
        if created_at_raw != "N/A":
            try:
                dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                if target_tz:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=target_tz)
                    else:
                        dt = dt.astimezone(target_tz)

                formatted = dt.strftime("%Y-%m-%d %H:%M:%S %z")
                if len(formatted) > 5 and formatted[-5] in ("+", "-"):
                    formatted = formatted[:-2] + ":" + formatted[-2:]
                created_at_local = formatted.strip()
            except Exception:
                created_at_local = created_at_raw
        else:
            created_at_local = "N/A"

        results.append(
            {
                "incident_id": incident.get("id", "N/A"),
                "incident_number": incident.get("incident_number", "N/A"),
                "incident_title": incident.get("title", "N/A"),
                "status": incident.get("status", "N/A"),
                "urgency": incident.get("urgency", "N/A"),
                "service": incident.get("service", {}).get("summary", "N/A"),
                "created_at": created_at_local,
                "trigger_summary": trigger_summary,
            }
        )

    return results


def export_to_csv(
    data: List[Dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_incidents",
) -> str:
    """Exports dataset to a safely versioned, timestamped CSV file[cite: 18, 19]."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = [
        "Incident ID",
        "Incident Number",
        "Incident Title",
        "Status",
        "Urgency",
        "Service",
        "Created At",
        "Trigger Summary",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if data:
            for row in data:
                mapped_row = {
                    "Incident ID": row.get("incident_id"),
                    "Incident Number": row.get("incident_number"),
                    "Incident Title": row.get("incident_title"),
                    "Status": row.get("status"),
                    "Urgency": row.get("urgency"),
                    "Service": row.get("service"),
                    "Created At": row.get("created_at"),
                    "Trigger Summary": row.get("trigger_summary"),
                }
                writer.writerow(mapped_row)

    logger.info(f"✓ CSV report saved: '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter that increases the spacing between flags and descriptions[cite: 18]."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Configures command line interface options[cite: 18]."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Incidents Details v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        help="Use default range (Last 7 days relative to exact current moment)",
    )
    parser.add_argument(
        "-s", "--since", help="Start date (YYYY-MM-DD or ISO 8601 string)"
    )
    parser.add_argument(
        "-u", "--until", help="End date (YYYY-MM-DD or ISO 8601 string)"
    )
    parser.add_argument(
        "-l",
        "--lookback",
        help="Relative lookback span (e.g., '2d', '3w', '1m', '1y')",
    )
    parser.add_argument(
        "-t",
        "--timezone",
        default="UTC",
        metavar="TZ",
        help="Custom timezone IANA name for relative calendar calculations (e.g., 'America/Santiago', 'UTC')",
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_incidents", help="Custom CSV filename prefix"
    )
    parser.add_argument(
        "-r", "--rate-limit", type=int, default=8, help="Max API requests per second"
    )
    return parser


def main() -> None:
    parser = build_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get("API_TOKEN")
    if not api_token or api_token.strip() == "YOUR_API_TOKEN_HERE":
        logger.error(
            "ERROR: Missing API token. Export PAGERDUTY_API_TOKEN environment variable."
        )
        sys.exit(1)

    target_tz = parse_timezone(args.timezone)
    now_local = datetime.now(target_tz)

    since, until = None, None

    if args.default:
        since_local = now_local - timedelta(days=7)
        until_local = now_local

        since = since_local.strftime("%Y-%m-%dT%H:%M:%S")
        until = until_local.strftime("%Y-%m-%dT%H:%M:%S")

    elif args.lookback:
        try:
            delta = parse_lookback_span(args.lookback)
            since_local = now_local - delta
            until_local = now_local
            
            since = since_local.strftime("%Y-%m-%dT%H:%M:%S")
            until = until_local.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError as e:
            logger.error(f"ERROR: {e}")
            sys.exit(1)

    else:
        since = args.since or os.environ.get("SINCE_DATE")
        until = args.until or os.environ.get("UNTIL_DATE")

        if not since or not until:
            logger.error(
                "ERROR: You must specify a time range via -d, -l <SPAN>, or -s/-u."
            )
            sys.exit(1)

        logger.info(f"Executing raw API time window: {since} -> {until}")

    try:
        api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        if not api.validate_token():
            sys.exit(1)

        start_time = time.time()

        incidents = api.get_incidents(since=since, until=until, time_zone=args.timezone)

        if not incidents:
            logger.warning("No incidents matched the target date range.")
            sys.exit(0)

        results = extract_incident_data(incidents, target_tz=target_tz)
        output_filename = export_to_csv(results, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✓ Processed {len(incidents)} incidents in {elapsed:.2f}s")
        print(f"✓ Report output: {output_filename}")
        print(f"{'='*60}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()