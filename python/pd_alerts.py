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

__version__ = "1.6.0"

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
    """PagerDuty REST API v2 Client for fetching alerts with native timezone support."""

    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token:
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
                "User-Agent": f"PagerDutyDevBuddy-AlertExporter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces client-side rate limiting ($Rate = 8\\text{ req/s}$)."""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()

    def _request(
        self, url: str, params: Optional[dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes an HTTP GET request with retry backoff."""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN."
                    )
                    sys.exit(1)

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    logger.error(
                        f"Request failed after {max_retries} attempts: {e}"
                    )
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

    def get_alerts(
        self,
        since: Optional[str] = None,
        until: Optional[str] = None,
        time_zone: Optional[str] = None,
    ) -> List[Dict]:
        """Fetches alerts using offset pagination, natively evaluated by PagerDuty's time_zone parameter
        when provided; otherwise the account's default time zone governs interpretation."""
        logger.info(f"Fetching alerts from native API window: {since or 'Beginning'} -> {until or 'Now'} (TZ: {time_zone or 'account default'})")
        alerts = []
        offset = 0
        limit = 100

        while True:
            params = {
                "offset": offset,
                "limit": limit,
            }
            if time_zone:
                params["time_zone"] = time_zone
            if since:
                params["since"] = since
            if until:
                params["until"] = until

            response = self._request(f"{self.base_url}/alerts", params=params)
            if not response:
                break

            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON response: {e}")
                break

            raw_alerts = data.get("alerts", [])
            for alert in raw_alerts:
                body = alert.get("body") or {}
                cef_details = body.get("cef_details") or {}
                details = body.get("details") or {}
                incident = alert.get("incident") or {}
                service = alert.get("service") or {}
                integration = alert.get("integration") or {}

                alerts.append(
                    {
                        "alert_id": alert.get("id"),
                        "alert_key": alert.get("alert_key"),
                        "summary": alert.get("summary"),
                        "status": alert.get("status"),
                        "severity": alert.get("severity"),
                        "created_at": format_datetime(alert.get("created_at")),
                        "resolved_at": format_datetime(alert.get("resolved_at")),
                        "suppressed": alert.get("suppressed"),
                        "incident_id": incident.get("id"),
                        "incident_summary": incident.get("summary"),
                        "service_id": service.get("id"),
                        "service_name": service.get("summary"),
                        "integration": integration.get("summary"),
                        "source_origin": cef_details.get("source_origin"),
                        "source_component": cef_details.get("source_component"),
                        "event_class": cef_details.get("event_class"),
                        "service_group": cef_details.get("service_group"),
                        "details": str(details) if details else "",
                        "html_url": alert.get("html_url"),
                    }
                )

            if len(alerts) > 0 and len(alerts) % 100 == 0:
                logger.info(f"Retrieved {len(alerts)} alerts...")

            if not data.get("more"):
                break

            offset += limit

        logger.info(f"✓ Total alerts retrieved: {len(alerts)}")
        return alerts


def format_datetime(dt_str: Optional[str]) -> str:
    """Reformats an ISO 8601 timestamp for display: 'T' becomes a space, and a missing
    offset (naive or 'Z') is made explicit as '+00:00' (UTC). The offset, if any, is
    taken as-is from the API response with no conversion applied."""
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


def export_to_csv(
    alerts: List[Dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_alerts",
) -> str:
    """Exports list of alert dictionaries to a safely versioned timestamped CSV file."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    # Construct dynamic collision-proof timestamped filename strictly following prefix_YYYYMMDD-HHMMSS.csv
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    # Use human-readable static column headers for robust downstream parsing
    fieldnames = [
        "Alert ID",
        "Alert Key",
        "Alert Summary",
        "Status",
        "Severity",
        "Created At",
        "Resolved At",
        "Suppressed",
        "Incident ID",
        "Incident Summary",
        "Service ID",
        "Service Name",
        "Integration",
        "Source Origin",
        "Source Component",
        "Event Class",
        "Service Group",
        "Details",
        "HTML URL",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        if alerts:
            for alert in alerts:
                mapped_row = {
                    "Alert ID": alert.get("alert_id"),
                    "Alert Key": alert.get("alert_key"),
                    "Alert Summary": alert.get("summary"),
                    "Status": alert.get("status"),
                    "Severity": alert.get("severity"),
                    "Created At": alert.get("created_at"),
                    "Resolved At": alert.get("resolved_at"),
                    "Suppressed": alert.get("suppressed"),
                    "Incident ID": alert.get("incident_id"),
                    "Incident Summary": alert.get("incident_summary"),
                    "Service ID": alert.get("service_id"),
                    "Service Name": alert.get("service_name"),
                    "Integration": alert.get("integration"),
                    "Source Origin": alert.get("source_origin"),
                    "Source Component": alert.get("source_component"),
                    "Event Class": alert.get("event_class"),
                    "Service Group": alert.get("service_group"),
                    "Details": alert.get("details"),
                    "HTML URL": alert.get("html_url"),
                }
                writer.writerow(mapped_row)

    logger.info(f"✓ CSV report saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options with explicit default, relative lookback, and custom timezone options."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Alerts v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-d",
        "--default",
        action="store_true",
        help="Use default range (Local Midnight 7 days ago -> exact moment now)",
    )
    parser.add_argument(
        "-s", "--since", help="Start date (YYYY-MM-DD or ISO-8601 string)"
    )
    parser.add_argument(
        "-u", "--until", help="End date (YYYY-MM-DD or ISO-8601 string)"
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
        help="Custom timezone IANA name (e.g., 'America/Santiago', 'UTC'). If omitted, dates are "
        "interpreted using the account's default time zone and output timestamps are rendered "
        "in UTC without an offset",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_alerts",
        help="Custom CSV filename prefix",
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=8,
        help="API limit rate in req/s",
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
        since_local = (now_local - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        until_local = now_local.replace(hour=23, minute=59, second=59, microsecond=0)

        since = since_local.strftime("%Y-%m-%dT%H:%M:%S")
        until = until_local.strftime("%Y-%m-%dT%H:%M:%S")

    elif args.lookback:
        try:
            delta = parse_lookback_span(args.lookback)
            since_local = (now_local - delta).replace(hour=0, minute=0, second=0, microsecond=0)
            until_local = now_local.replace(hour=23, minute=59, second=59, microsecond=0)

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

        alerts = api.get_alerts(since=since, until=until, time_zone=args.timezone)
        
        output_filename = export_to_csv(alerts, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"Time Window ({args.timezone or 'account default'}): {since or 'Beginning'} -> {until or 'Now'}")
        print(f"✓ Processed {len(alerts)} records in {elapsed:.2f}s")
        print(f"✓ Report saved to '{output_filename}'")
        print(f"{'='*50}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()