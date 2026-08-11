#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, List, Optional
import requests

__version__ = "1.4.0"

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


class PagerDutyExporter:
    """PagerDuty REST API v2 client for exporting incident MTTA/MTTR metrics."""

    def __init__(self, api_token: str, rate_limit: int = 4):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.rate_limiter = ThreadSafeRateLimiter(requests_per_second=rate_limit)
        self.service_cache: Dict[str, str] = {}
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

    def _make_request(
        self, method: str, endpoint: str, params: Optional[Dict] = None
    ) -> Optional[requests.Response]:
        """Make an API request with rate limiting and exponential backoff retries."""
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.acquire()
                response = self.session.request(
                    method, url, params=params, timeout=self.timeout
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited ($Rate = {self.rate_limiter.min_interval:.3f}\\text{{s/req}}$). Waiting {retry_after}s..."
                    )
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
        """Validate API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._make_request("GET", "users", params={"limit": 1})
        if response is not None and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def get_incidents(
        self, since: Optional[str] = None, until: Optional[str] = None
    ) -> List[Dict]:
        """Fetch resolved incidents using offset pagination and dynamic time windows."""
        incidents = []
        offset = 0
        limit = 100

        logger.info(f"Fetching resolved incidents from window: {since or 'Beginning'} -> {until or 'Now'}")

        while True:
            params = {
                "statuses[]": "resolved",
                "limit": limit,
                "offset": offset,
                "total": True,
            }
            if since:
                params["since"] = since
            if until:
                params["until"] = until

            response = self._make_request("GET", "incidents", params=params)
            if not response:
                logger.error("Failed to fetch incident batch.")
                break

            data = response.json()
            fetched_batch = data.get("incidents", [])
            incidents.extend(fetched_batch)

            if not data.get("more", False):
                break

            offset += limit
            logger.info(f"Fetched {len(incidents)} incidents so far...")

        logger.info(f"✓ Total resolved incidents retrieved: {len(incidents)}")
        return incidents

    def get_service_name(self, service_id: str) -> str:
        """Fetch service name with local memory caching to avoid redundant API queries."""
        if service_id in self.service_cache:
            return self.service_cache[service_id]

        response = self._make_request("GET", f"services/{service_id}")
        if response and response.status_code == 200:
            service_data = response.json().get("service") or {}
            service_name = service_data.get("name", "Unknown Service")
            self.service_cache[service_id] = service_name
            return service_name

        return "Unknown Service"

    def get_incident_log_entries(self, incident_id: str) -> List[Dict]:
        """Fetch all log entries for a specific incident."""
        response = self._make_request("GET", f"incidents/{incident_id}/log_entries")
        if response and response.status_code == 200:
            return response.json().get("log_entries", [])
        return []

    def calculate_time_metrics(
        self, incident: Dict, log_entries: List[Dict]
    ) -> Dict:
        """Calculate MTTA and MTTR metrics using first acknowledgment time with safe dictionary accesses."""
        created_at_dt = datetime.fromisoformat(
            incident["created_at"].replace("Z", "+00:00")
        )

        first_ack_time = None
        first_acknowledger = None
        resolve_time = None
        all_acknowledgers = []
        resolver = None

        sorted_entries = sorted(log_entries, key=lambda x: x.get("created_at", ""))

        for entry in sorted_entries:
            entry_type = entry.get("type")

            agent_data = entry.get("agent") or {}
            agent = agent_data.get("summary", "System / Automated Action")

            if entry_type == "acknowledge_log_entry":
                if first_ack_time is None:
                    first_ack_time = datetime.fromisoformat(
                        entry["created_at"].replace("Z", "+00:00")
                    )
                    first_acknowledger = agent
                all_acknowledgers.append(agent)

            elif entry_type == "resolve_log_entry":
                resolve_time = datetime.fromisoformat(
                    entry["created_at"].replace("Z", "+00:00")
                )
                resolver = agent

        mtta = (first_ack_time - created_at_dt) if first_ack_time else None
        mttr = (resolve_time - created_at_dt) if resolve_time else None

        return {
            "first_acknowledger": first_acknowledger or "No acknowledgment",
            "all_acknowledgers": (
                ", ".join(sorted(set(all_acknowledgers)))
                if all_acknowledgers
                else "No acknowledgment"
            ),
            "resolver": resolver or "Not resolved",
            "mtta": self._format_timedelta(mtta),
            "mttr": self._format_timedelta(mttr),
        }

    @staticmethod
    def _format_timedelta(td: Optional[timedelta]) -> str:
        """Format timedelta into HH:MM:SS string."""
        if not td:
            return "N/A"

        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def process_single_incident(self, incident: Dict) -> Optional[Dict]:
        """Process a single incident and extract MTTA/MTTR metrics."""
        try:
            log_entries = self.get_incident_log_entries(incident["id"])
            metrics = self.calculate_time_metrics(incident, log_entries)

            service_data = incident.get("service") or {}
            service_id = service_data.get("id", "")
            service_name = (
                self.get_service_name(service_id) if service_id else "Unknown Service"
            )

            created_at_raw = incident.get("created_at", "N/A")

            return {
                "Incident ID": incident.get("id", "N/A"),
                "Title": incident.get("title", "N/A"),
                "Created At": created_at_raw,
                "Service Name": service_name,
                "First Acknowledger": metrics["first_acknowledger"],
                "All Acknowledger(s)": metrics["all_acknowledgers"],
                "Resolver": metrics["resolver"],
                "MTTA (HH:MM:SS)": metrics["mtta"],
                "MTTR (HH:MM:SS)": metrics["mttr"],
            }
        except Exception as e:
            logger.error(f"Error processing incident {incident.get('id')}: {e}")
            return None


def export_to_csv(
    data: List[Dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_metrics"
) -> Optional[str]:
    """Exports processed incident metrics to a safely versioned timestamped CSV file."""
    if not data:
        logger.info("No data available to export.")
        return None

    # 1. Resolve fallback hierarchy: Explicit CLI arg -> Environment Var -> Default
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    # 2. Sanitize extension if user explicitly passed `.csv`
    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    # 3. Construct dynamic collision-proof timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = [
        "Incident ID",
        "Title",
        "Created At",
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
        description=f"CSE - PagerDuty Incident MTTA/MTTR Metrics Live (SLOW) v{__version__}",
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
        help="Custom timezone offset or IANA name for relative lookback calculations (e.g., 'America/New_York', '-05:00', 'UTC')",
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_metrics", help="Custom CSV filename prefix"
    )
    parser.add_argument(
        "-w",
        "--max-workers",
        type=int,
        default=5,
        help="Maximum concurrent worker threads (1-10)",
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=4,
        help="Maximum API requests per second",
    )
    return parser


def main() -> None:
    parser = build_parser()

    # Automatically show help and exit if no CLI arguments are supplied
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

    if args.max_workers < 1 or args.max_workers > 10:
        logger.error("Max workers must be between 1 and 10.")
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
        exporter = PagerDutyExporter(api_token, rate_limit=args.rate_limit)
        if not exporter.validate_token():
            sys.exit(1)

        start_time = time.time()

        # Pass dynamic window variables directly to the incident fetcher
        incidents = exporter.get_incidents(since=since, until=until)

        if not incidents:
            logger.warning("No incidents found for the specified date range.")
            sys.exit(0)

        logger.info(
            f"Processing {len(incidents)} incidents with {args.max_workers} workers..."
        )
        processed_incidents = []

        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            future_to_incident = {
                executor.submit(exporter.process_single_incident, incident): incident
                for incident in incidents
            }

            completed = 0
            for future in as_completed(future_to_incident):
                completed += 1
                if completed % 10 == 0 or completed == len(incidents):
                    logger.info(
                        f"Progress: {completed}/{len(incidents)} incidents processed"
                    )

                result = future.result()
                if result:
                    processed_incidents.append(result)

        # Sort incidents chronologically by creation timestamp
        logger.info("Sorting incidents chronologically by 'Created At' timestamp...")
        processed_incidents.sort(key=lambda x: x.get("Created At", ""))

        # Utilize safely isolated output writing
        output_filename = export_to_csv(processed_incidents, prefix=args.output)
        elapsed = time.time() - start_time

        print("\n" + "=" * 70)
        print("PROCESSING SUMMARY".center(70))
        print("=" * 70)
        print(f"Time Window:         {since or 'Beginning'} -> {until or 'Now'}")
        print(f"Incidents Processed: {len(processed_incidents)}/{len(incidents)}")
        print(f"Execution Time:      {elapsed:.2f}s")
        print(f"Output File:         {output_filename or 'N/A'}")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()