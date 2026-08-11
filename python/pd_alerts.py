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

__version__ = "1.3.0"

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


class PagerDutyAPI:
    """PagerDuty REST API v2 Client for fetching alerts."""

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

    def get_alerts(
        self, since: Optional[str] = None, until: Optional[str] = None
    ) -> List[Dict]:
        """Fetches alerts using offset pagination checking the `more` attribute."""
        logger.info(f"Fetching alerts from window: {since or 'Beginning'} -> {until or 'Now'}")
        alerts = []
        offset = 0
        limit = 100

        while True:
            params = {"offset": offset, "limit": limit}
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
    """Formats ISO datetime string to standard readable format."""
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return dt_str


def export_to_csv(
    alerts: List[Dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_alerts",
) -> str:
    """Exports list of alert dictionaries to a safely versioned timestamped CSV file."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = [
        "alert_id",
        "alert_key",
        "summary",
        "status",
        "severity",
        "created_at",
        "resolved_at",
        "suppressed",
        "incident_id",
        "incident_summary",
        "service_id",
        "service_name",
        "source_origin",
        "source_component",
        "event_class",
        "service_group",
        "details",
        "html_url",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if alerts:
            writer.writerows(alerts)

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
        help="Use default range (Last 7 days relative to exact current target timezone time)",
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
        default="UTC",
        metavar="TZ",
        help="Custom timezone offset or IANA name for relative lookback calculations (e.g., 'America/New_York', '-05:00', 'UTC')",
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
    return parser


def main() -> None:
    parser = build_parser()

    # Automatically show help and exit if no CLI arguments are supplied
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get(
        "API_TOKEN"
    )
    if not api_token or api_token.strip() == "YOUR_API_TOKEN_HERE":
        logger.error(
            "ERROR: Missing API token. Export PAGERDUTY_API_TOKEN environment variable."
        )
        sys.exit(1)

    target_tz = parse_timezone(args.timezone)
    until_local = datetime.now(target_tz)

    since, until = None, None

    if args.default:
        since_local = until_local - timedelta(days=7)
        since = since_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        until = until_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(
            f"Executing default lookback window (TZ={args.timezone}): {since} -> {until}"
        )

    elif args.lookback:
        try:
            delta = parse_lookback_span(args.lookback)
            since_local = until_local - delta
            since = since_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            until = until_local.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(
                f"Executing dynamic lookback span '{args.lookback}' (TZ={args.timezone}): {since} -> {until}"
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
        api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        start_time = time.time()

        alerts = api.get_alerts(since=since, until=until)
        output_filename = export_to_csv(alerts, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
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