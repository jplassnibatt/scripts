#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, List, Optional
import requests

__version__ = "1.6.0"

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


class ThreadSafeRateLimiter:
    """Thread-safe rate limiter for client-side request throttling."""

    def __init__(self, requests_per_second: int = 4):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Wait if necessary to respect rate limits."""
        with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time

            if time_since_last < self.min_interval:
                time.sleep(self.min_interval - time_since_last)

            self.last_request_time = time.time()


class PagerDutyAnalyzer:
    """PagerDuty REST API v2 Client for optimized incident alert analysis."""

    def __init__(self, api_token: str, rate_limit: int = 4):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.max_time_range_days = 180
        self.rate_limiter = ThreadSafeRateLimiter(requests_per_second=rate_limit)
        self.max_retries = 3

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-AlertAnalyzer/{__version__}",
            }
        )

    def _request(
        self, url: str, params: Optional[Dict] = None
    ) -> Optional[requests.Response]:
        """Makes an HTTP GET request with robust error handling and rate-limit backoff."""
        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.acquire()
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited ($Rate = {self.rate_limiter.min_interval:.3f}\\text{{s/req}}$). Waiting {wait}s..."
                    )
                    time.sleep(wait)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN."
                    )
                    sys.exit(1)

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Timeout encountered (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error("Request timed out after maximum retries.")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
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

    def get_incidents_for_timerange(self, since: str, until: str, time_zone: str = "UTC") -> List[Dict]:
        """Fetches incidents natively evaluated by PagerDuty's time_zone handler."""
        incidents = []
        offset = 0
        limit = 100

        while True:
            params = {
                "since": since,
                "until": until,
                "limit": limit,
                "offset": offset,
                "sort_by": "created_at:desc",
                "total": True,
                "time_zone": time_zone,
            }

            response = self._request(f"{self.base_url}/incidents", params=params)
            if not response:
                break

            data = response.json()
            fetched = data.get("incidents", [])
            incidents.extend(fetched)

            if not data.get("more", False):
                break

            offset += limit

        return incidents

    def get_all_incidents(self, since_date: str, until_date: str, time_zone: str = "UTC") -> List[Dict]:
        """Handles PagerDuty's 6-month max date range constraint using naive local boundaries."""
        all_incidents = []

        def parse_dt(d_str: str) -> datetime:
            if "T" in d_str:
                return datetime.fromisoformat(d_str.replace("Z", "+00:00"))
            return datetime.strptime(d_str, "%Y-%m-%d")

        try:
            start_dt = parse_dt(since_date)
            end_dt = parse_dt(until_date)
        except ValueError:
            logger.error(
                f"Invalid date format: {since_date} or {until_date}. Use YYYY-MM-DD or ISO-8601."
            )
            sys.exit(1)

        current_start = start_dt
        while current_start < end_dt:
            current_end = min(
                current_start + timedelta(days=self.max_time_range_days), end_dt
            )

            chunk_since = current_start.strftime("%Y-%m-%dT%H:%M:%S")
            chunk_until = current_end.strftime("%Y-%m-%dT%H:%M:%S")

            logger.info(f"Fetching chunk: {chunk_since} -> {chunk_until} (TZ: {time_zone})")

            chunk_incidents = self.get_incidents_for_timerange(
                chunk_since, chunk_until, time_zone
            )
            all_incidents.extend(chunk_incidents)

            current_start = current_end + timedelta(seconds=1)

        logger.info(f"✓ Retrieved total of {len(all_incidents)} incidents")
        return all_incidents

    def analyze_incidents(
        self, incidents: List[Dict], target_tz: Optional[tzinfo] = None
    ) -> Dict[str, Dict]:
        """Filters and analyzes incidents, extracting natively localized timestamps."""
        alert_counts = {}
        logger.info("Analyzing incidents for multiple alerts...")

        for incident in incidents:
            alert_data = incident.get("alert_counts") or {}
            num_alerts = alert_data.get("all", 0)

            if num_alerts >= 2:
                service_data = incident.get("service") or {}
                
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
                        if len(formatted) > 5 and formatted[-5] in ('+', '-'):
                            formatted = formatted[:-2] + ":" + formatted[-2:]
                        created_at_local = formatted.strip()
                    except Exception:
                        created_at_local = created_at_raw
                else:
                    created_at_local = "N/A"

                alert_counts[incident["id"]] = {
                    "alert_count": num_alerts,
                    "title": incident.get("title", "N/A"),
                    "status": incident.get("status", "N/A"),
                    "created_at": created_at_local,
                    "urgency": incident.get("urgency", "N/A"),
                    "service": service_data.get("summary", "N/A"),
                }

        logger.info(
            f"✓ Found {len(alert_counts)} incidents containing multiple alerts"
        )
        return alert_counts


def format_period_datetime(dt_str: str, tz: tzinfo) -> str:
    """Formats period date string with a colonized UTC offset."""
    if not dt_str:
        return dt_str
    try:
        cleaned = dt_str.replace("Z", "+00:00")
        if "T" in cleaned:
            dt = datetime.fromisoformat(cleaned)
        else:
            dt = datetime.strptime(cleaned, "%Y-%m-%d")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        else:
            dt = dt.astimezone(tz)

        formatted = dt.strftime("%Y-%m-%d %H:%M:%S %z")
        if len(formatted) > 5 and formatted[-5] in ("+", "-"):
            formatted = formatted[:-2] + ":" + formatted[-2:]
        return formatted.strip()
    except Exception:
        return dt_str.replace("T", " ")


def export_to_csv(
    alert_counts: Dict[str, Dict], 
    since: str, 
    until: str, 
    total: int,
    time_zone: str,
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_incident_grouped_alerts",
) -> str:
    """Exports data to a safely versioned timestamped CSV file."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    total_alerts = sum(inc["alert_count"] for inc in alert_counts.values())

    target_tz = parse_timezone(time_zone)
    display_since = format_period_datetime(since, target_tz)
    display_until = format_period_datetime(until, target_tz)

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Time Period Start", display_since])
        writer.writerow(["Time Period End", display_until])
        writer.writerow(["Total Incidents", total])
        writer.writerow(["Total Incidents with Multiple Alerts", len(alert_counts)])
        writer.writerow(["Total Number of Grouped Alerts", total_alerts])
        writer.writerow([])

        writer.writerow(["DETAILS"])
        
        writer.writerow(
            [
                "Incident ID",
                "Title",
                "Service",
                "Status",
                "Created At",
                "Urgency",
                "Number of Alerts",
            ]
        )

        for inc_id, data in sorted(
            alert_counts.items(), key=lambda x: x[1]["alert_count"], reverse=True
        ):
            writer.writerow(
                [
                    inc_id,
                    data["title"],
                    data["service"],
                    data["status"],
                    data["created_at"],
                    data["urgency"],
                    data["alert_count"],
                ]
            )

    logger.info(f"✓ CSV output saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options with explicit default, relative lookback, and custom timezone options."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Grouped Alerts v{__version__}",
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
        help="Custom timezone IANA name for relative calendar calculations (e.g., 'America/Santiago', 'UTC')",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_incident_grouped_alerts",
        help="Custom CSV filename prefix",
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=8,
        help="Maximum API requests per second",
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

        since = apply_default_time_if_missing(since)
        until = apply_default_time_if_missing(until)

        logger.info(f"Executing raw API time window: {since} -> {until}")

    try:
        analyzer = PagerDutyAnalyzer(api_token, rate_limit=args.rate_limit)
        if not analyzer.validate_token():
            sys.exit(1)

        start_time = time.time()

        incidents = analyzer.get_all_incidents(since, until, time_zone=args.timezone)
        if not incidents:
            logger.warning("No incidents found in the specified time range.")
            sys.exit(0)

        alert_counts = analyzer.analyze_incidents(incidents, target_tz=target_tz)
        output_filename = export_to_csv(
            alert_counts, 
            since=since, 
            until=until, 
            total=len(incidents), 
            time_zone=args.timezone,
            prefix=args.output
        )

        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✓ Processed {len(incidents)} total incidents in {elapsed:.2f}s")
        print(f"✓ Report saved to '{output_filename}'")
        print(f"{'='*60}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()