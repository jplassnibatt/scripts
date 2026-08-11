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

__version__ = "1.4.1"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
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
            f"Invalid timezone identifier: '{tz_str}'. Use IANA format (e.g., 'America/New_York') or offset (e.g., '-05:00', '+02:00', 'UTC')."
        )
        sys.exit(1)


class PagerDutyAnalyticsExporter:
    """PagerDuty Analytics API client utilizing optimized bulk endpoints for MTTA/MTTR metrics."""

    def __init__(self, api_token: str, rate_limit: int = 4):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.min_interval = 1.0 / rate_limit
        self.last_request = 0.0
        self.max_retries = 3
        self.timeout = 30

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-MetricsExporter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces basic client-side rate limiting."""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()

    def _request(
        self, method: str, endpoint: str, json_body: Optional[Dict] = None, additional_headers: Optional[Dict] = None
    ) -> Optional[requests.Response]:
        """Makes an API request with rate limiting and exponential backoff retries."""
        url = f"{self.base_url}/{endpoint}"
        
        request_headers = self.session.headers.copy()
        if additional_headers:
            request_headers.update(additional_headers)

        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                response = requests.request(
                    method, url, json=json_body, headers=request_headers, timeout=self.timeout
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN permissions."
                    )
                    sys.exit(1)

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    logger.error("Request timed out after maximum retries.")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    return None

        return None

    def validate_token(self) -> bool:
        """Validate API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._request("GET", "users")
        if response is not None and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def get_analytics_incidents(
        self, since: Optional[str] = None, until: Optional[str] = None, time_zone: str = "UTC"
    ) -> List[Dict]:
        """Fetch pre-calculated incident metrics via POST /analytics/raw/incidents using native timezone delegation."""
        incidents = []
        limit = 1000

        logger.info(f"Fetching enriched analytics data from window: {since or 'Beginning'} -> {until or 'Now'} (TZ: {time_zone})")

        custom_headers = {"time-zone": time_zone}

        body = {
            "limit": limit,
            "order": "asc",
            "order_by": "created_at",
            "time_zone": time_zone,
            "filters": {}
        }

        if since:
            body["filters"]["created_at_start"] = since
        if until:
            body["filters"]["created_at_end"] = until

        while True:
            response = self._request("POST", "analytics/raw/incidents", json_body=body, additional_headers=custom_headers)
            if not response:
                logger.error("Failed to fetch analytics batch.")
                break

            data = response.json()
            fetched_batch = data.get("data", [])
            incidents.extend(fetched_batch)

            if not data.get("more", False):
                break

            body["starting_after"] = data.get("last")
            logger.info(f"Fetched {len(incidents)} incidents so far...")

        logger.info(f"✓ Total analytics records retrieved: {len(incidents)}")
        return incidents


def format_timedelta(total_seconds: Optional[int]) -> str:
    """Format total seconds into an HH:MM:SS string."""
    if total_seconds is None:
        return "N/A"

    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def process_analytics_data(raw_incidents: List[Dict], time_zone: str) -> List[Dict]:
    """Map raw analytics data to CSV columns cleanly and format localized timestamps."""
    processed = []
    created_at_key = f"Created At_{time_zone}"

    for inc in raw_incidents:
        ack_users = inc.get("acknowledged_user_names") or []
        first_ack = ack_users[0] if ack_users else "No acknowledgment"
        all_acks = ", ".join(ack_users) if ack_users else "No acknowledgment"

        created_at_raw = inc.get("created_at", "N/A")
        if created_at_raw != "N/A":
            try:
                dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                created_at_local = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                created_at_local = created_at_raw
        else:
            created_at_local = "N/A"

        processed.append({
            "Incident ID": inc.get("id", "N/A"),
            "Title": inc.get("description", "N/A"),
            created_at_key: created_at_local,
            "Service Name": inc.get("service_name", "N/A"),
            "First Acknowledger": first_ack,
            "All Acknowledger(s)": all_acks,
            "Resolver": inc.get("resolved_by_user_name") or "Not resolved",
            "MTTA (HH:MM:SS)": format_timedelta(inc.get("seconds_to_first_ack")),
            "MTTR (HH:MM:SS)": format_timedelta(inc.get("seconds_to_resolve")),
        })

    return processed


def export_to_csv(
    data: List[Dict],
    time_zone: str,
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_analytics_metrics"
) -> Optional[str]:
    """Exports processed incident metrics to a safely versioned timestamped CSV file."""
    if not data:
        logger.info("No data available to export.")
        return None

    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = [
        "Incident ID",
        "Title",
        f"Created At_{time_zone}",
        "Service Name",
        "First Acknowledger",
        "All Acknowledger(s)",
        "Resolver",
        "MTTA (HH:MM:SS)",
        "MTTR (HH:MM:SS)",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"✓ Metrics report saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options with explicit default, relative lookback, and custom timezone options."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Incident MTTA/MTTR Analytics Historic (FAST with 24 hours delay) v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        help="Use default range (Local Midnight 7 days ago -> exact end of target timezone day)",
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
        help="Custom timezone IANA name for relative calendar calculations (e.g., 'America/New_York', 'UTC')",
    )
    parser.add_argument(
        "-o", "--output", help="Output CSV filename prefix"
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=4,
        help="API limit rate in req/s (default: 4)",
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
        since_local = (now_local - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        until_local = now_local.replace(hour=23, minute=59, second=59, microsecond=0)
        
        since = since_local.strftime("%Y-%m-%dT%H:%M:%S")
        until = until_local.strftime("%Y-%m-%dT%H:%M:%S")
        logger.info(
            f"Executing default calendar window (TZ={args.timezone}): {since} -> {until}"
        )

    elif args.lookback:
        try:
            delta = parse_lookback_span(args.lookback)
            since_local = (now_local - delta).replace(hour=0, minute=0, second=0, microsecond=0)
            until_local = now_local.replace(hour=23, minute=59, second=59, microsecond=0)
            
            since = since_local.strftime("%Y-%m-%dT%H:%M:%S")
            until = until_local.strftime("%Y-%m-%dT%H:%M:%S")
            logger.info(
                f"Executing dynamic calendar span '{args.lookback}' (TZ={args.timezone}): {since} -> {until}"
            )
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
        exporter = PagerDutyAnalyticsExporter(api_token, rate_limit=args.rate_limit)
        if not exporter.validate_token():
            sys.exit(1)

        start_time = time.time()

        raw_incidents = exporter.get_analytics_incidents(since=since, until=until, time_zone=args.timezone)

        if not raw_incidents:
            logger.warning("No incidents found for the specified date range.")
            sys.exit(0)

        processed_incidents = process_analytics_data(raw_incidents, time_zone=args.timezone)

        output_filename = export_to_csv(processed_incidents, time_zone=args.timezone, prefix=args.output)
        elapsed = time.time() - start_time

        print("\n" + "=" * 70)
        print("PROCESSING SUMMARY".center(70))
        print("=" * 70)
        print(f"Time Window ({args.timezone}): {since or 'Beginning'} -> {until or 'Now'}")
        print(f"Incidents Processed:   {len(processed_incidents)}")
        print(f"Execution Time:        {elapsed:.2f}s")
        print(f"Output File:           {output_filename or 'N/A'}")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()