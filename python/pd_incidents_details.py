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

__version__ = "1.5.1"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_lookback_span(span_str: str) -> timedelta:
    """Parses dynamic lookback strings (e.g., '2d', '3w', '1m', '1y') into a timedelta."""
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
    """Parses timezone strings into tzinfo objects (supports UTC, offsets like +05:00/-08:00, or IANA names)."""
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


def apply_default_time_if_missing(date_str: str) -> str:
    """Stamps a bare 'YYYY-MM-DD' string with midnight (00:00:00). The API evaluates
    this naive time against the request's own time_zone parameter, so midnight lands
    on the correct wall-clock offset for whichever timezone is in effect."""
    candidate = date_str.strip()
    if "T" in candidate or " " in candidate:
        return candidate

    try:
        datetime.strptime(candidate, "%Y-%m-%d")
    except ValueError:
        return candidate

    return f"{candidate}T00:00:00"


class PagerDutyAPI:
    """PagerDuty REST API v2 client with built-in rate-limiting and session management."""

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
        """Enforces client-side rate limiting."""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()

    def _request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes API request with exponential backoff and rate-limit mitigation."""
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
        """Validates API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._request(f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def get_incidents(
        self, since: Optional[str] = None, until: Optional[str] = None, time_zone: Optional[str] = None
    ) -> List[Dict]:
        """Fetches all incidents within the specified date range."""
        incidents = []
        offset = 0
        limit = 100

        logger.info(f"Fetching incidents from native API window: {since or 'Beginning'} -> {until or 'Now'} (TZ: {time_zone or 'account default'})")

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "include[]": ["first_trigger_log_entry"],
            }
            if time_zone:
                params["time_zone"] = time_zone
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

    def get_incident_custom_fields(self, incident_id: str) -> Dict[str, str]:
        """Fetches custom field values for a specific incident from GET /incidents/{id}/custom_fields/values."""
        url = f"{self.base_url}/incidents/{incident_id}/custom_fields/values"
        response = self._request(url)
        if not response:
            return {}

        data = response.json()
        custom_fields = {}
        for item in data.get("custom_fields", []):
            field_name = item.get("display_name") or item.get("name")
            value = item.get("value")
            if isinstance(value, list):
                value = ", ".join(map(str, value))
            custom_fields[field_name] = str(value) if value is not None else ""

        return custom_fields


def format_datetime(dt_str: Optional[str]) -> str:
    """Reformats an ISO 8601 timestamp for display."""
    if not dt_str:
        return ""
    match = re.match(
        r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:?\d{2})?$",
        dt_str.strip(),
    )
    if not match:
        return dt_str

    date_part, time_part, offset = match.groups()
    if not offset or offset == "Z":
        offset = "+00:00"
    elif ":" not in offset:
        offset = f"{offset[:3]}:{offset[3:]}"

    return f"{date_part} {time_part} {offset}"


def extract_incident_data(incidents: List[Dict], api: Optional[PagerDutyAPI] = None) -> List[Dict]:
    """Extracts and flattens incident records, including Custom Fields retrieved per incident."""
    results = []

    for idx, incident in enumerate(incidents, 1):
        incident_id = incident.get("id", "N/A")
        logger.info(f"Extracting custom fields [{idx}/{len(incidents)}]: {incident_id}")

        trigger_summary = "N/A"
        if incident.get("first_trigger_log_entry"):
            trigger_summary = incident["first_trigger_log_entry"].get("summary", "N/A")

        created_at_raw = incident.get("created_at", "N/A")
        created_at_local = (
            format_datetime(created_at_raw) if created_at_raw != "N/A" else "N/A"
        )

        custom_fields = {}
        if api and incident_id != "N/A":
            custom_fields = api.get_incident_custom_fields(incident_id)

        results.append(
            {
                "incident_id": incident_id,
                "incident_number": incident.get("incident_number", "N/A"),
                "incident_title": incident.get("title", "N/A"),
                "status": incident.get("status", "N/A"),
                "urgency": incident.get("urgency", "N/A"),
                "service": incident.get("service", {}).get("summary", "N/A"),
                "created_at": created_at_local,
                "trigger_summary": trigger_summary,
                "custom_fields": custom_fields,
            }
        )

    return results


def export_to_csv(
    data: List[Dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_incidents",
) -> str:
    """Exports dataset to a safely versioned, timestamped CSV file with dynamic Custom Field columns."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    standard_headers = [
        "Incident ID",
        "Incident Number",
        "Incident Title",
        "Status",
        "Urgency",
        "Service",
        "Created At",
        "Trigger Summary",
    ]

    # Dynamically extract all unique Custom Field names across all retrieved incidents
    cf_keys = sorted({key for row in data for key in row.get("custom_fields", {}).keys()})
    cf_headers = [f"CF: {key}" for key in cf_keys]

    fieldnames = standard_headers + cf_headers

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if data:
            for row in data:
                cf_dict = row.get("custom_fields", {})
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
                for key in cf_keys:
                    mapped_row[f"CF: {key}"] = cf_dict.get(key, "")

                writer.writerow(mapped_row)

    logger.info(f"✓ CSV report saved: '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter that increases the spacing between flags and descriptions."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Configures command line interface options."""
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
        default=None,
        metavar="TZ",
        help="Custom timezone IANA name (e.g., 'America/Santiago', 'UTC').",
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_incidents", help="Custom CSV filename prefix"
    )
    parser.add_argument(
        "-r", "--rate-limit", type=int, default=8, help="Max API requests per second"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed [INFO] level log messages",
    )
    return parser


def main() -> None:
    parser = build_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    logger.setLevel(logging.INFO if args.debug else logging.WARNING)

    api_token = os.environ.get("PAGERDUTY_API_TOKEN")
    if not api_token or api_token.strip() == "YOUR_API_TOKEN_HERE":
        logger.error(
            "Set it using: export PAGERDUTY_API_TOKEN='your-api-token-here'"
        )
        sys.exit(1)

    target_tz = parse_timezone(args.timezone) if args.timezone else None
    now_local = datetime.now(target_tz or timezone.utc)

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

        since = apply_default_time_if_missing(since)
        until = apply_default_time_if_missing(until)

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

        results = extract_incident_data(incidents, api=api)
        output_filename = export_to_csv(results, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"Time Window ({args.timezone or 'account default'}): {since or 'Beginning'} -> {until or 'Now'}")
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