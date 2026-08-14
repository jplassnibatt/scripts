#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Dict, List, Optional
import requests

__version__ = "1.7.0"

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
            f"Invalid timezone identifier: '{tz_str}'. Use IANA format (e.g., 'America/Santiago', 'UTC')."
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
    """PagerDuty REST API v2 Client with dynamic rate-limit handling and resource resolution."""

    def __init__(self, api_token: str):
        if not api_token:
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-MTTRExporter/{__version__}",
            }
        )

    def _handle_rate_limits(self, response: requests.Response) -> None:
        """Handles dynamic API rate limits based on response headers."""
        remaining = int(response.headers.get("X-Rate-Limit-Remaining", 400))
        if remaining <= 1:
            wait_time = max(int(response.headers.get("X-Rate-Limit-Reset", 1)), 1)
            logger.warning(f"Rate limit approached. Sleeping for {wait_time}s...")
            time.sleep(wait_time + 1)

    def _request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes an HTTP GET request with retry backoff and header-based rate limiting."""
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = self.session.get(url, params=params, timeout=30)
                self._handle_rate_limits(response)

                if response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", 30))
                    logger.warning(f"HTTP 429 Too Many Requests. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN."
                    )
                    sys.exit(1)

                if response.status_code >= 500:
                    retry_count += 1
                    wait = 2 ** retry_count
                    logger.warning(f"Server error (HTTP {response.status_code}). Retrying in {wait}s...")
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Request failed after {max_retries} attempts: {e}")
                    return None
                wait = 2 ** retry_count
                time.sleep(wait)
        return None

    def validate_token(self) -> bool:
        """Validates API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._request(f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def resolve_service_identifiers(self, identifiers: List[str]) -> List[str]:
        """Translates a mix of Service Names and IDs into pure Service IDs."""
        resolved_ids = []
        for identifier in identifiers:
            if len(identifier) == 7 and identifier.startswith("P"):
                resolved_ids.append(identifier)
                continue

            logger.info(f"Resolving service name '{identifier}' to ID...")
            params = {"query": identifier, "limit": 1}
            response = self._request(f"{self.base_url}/services", params=params)
            
            if response:
                services = response.json().get("services", [])
                if services:
                    resolved_id = services[0].get("id")
                    logger.info(f"✓ Matched '{identifier}' to Service ID '{resolved_id}'")
                    resolved_ids.append(resolved_id)
                else:
                    logger.error(f"Could not find a service matching '{identifier}'. Skipping.")
        return resolved_ids

    def fetch_resolved_incidents(
        self, since: str, until: str, service_ids: Optional[List[str]] = None, time_zone: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetches resolved incidents using 6-month chunking natively evaluated by time_zone."""
        all_incidents = []
        
        def parse_dt(d_str: str) -> datetime:
            if "T" in d_str:
                return datetime.fromisoformat(d_str.replace("Z", "+00:00"))
            return datetime.strptime(d_str, "%Y-%m-%d")

        try:
            start_dt = parse_dt(since)
            end_dt = parse_dt(until)
        except ValueError:
            logger.error(
                f"Invalid date format: {since} or {until}. Use YYYY-MM-DD or ISO-8601."
            )
            sys.exit(1)

        if start_dt > end_dt:
            logger.error("Start date must be before end date.")
            sys.exit(1)

        chunk_start = start_dt
        while chunk_start < end_dt:
            chunk_end = min(chunk_start + timedelta(days=180), end_dt)
            
            chunk_since = chunk_start.strftime("%Y-%m-%dT%H:%M:%S")
            chunk_until = chunk_end.strftime("%Y-%m-%dT%H:%M:%S")
            
            logger.info(
                f"Fetching chunk: {chunk_since} -> {chunk_until} (TZ: {time_zone or 'account default'})"
            )

            offset = 0
            limit = 100
            params = {
                "since": chunk_since,
                "until": chunk_until,
                "statuses[]": ["resolved"],
            }
            if time_zone:
                params["time_zone"] = time_zone
            if service_ids:
                params["service_ids[]"] = service_ids

            while True:
                params["offset"] = offset
                params["limit"] = limit

                response = self._request(f"{self.base_url}/incidents", params=params)
                if not response:
                    break

                try:
                    data = response.json()
                except Exception as e:
                    logger.error(f"JSON parsing error: {e}")
                    break

                incidents = data.get("incidents", [])
                all_incidents.extend(incidents)

                if len(all_incidents) > 0 and len(all_incidents) % 500 == 0:
                    logger.info(f"Retrieved {len(all_incidents)} incidents...")

                if not data.get("more"):
                    break
                
                offset += limit

            chunk_start = chunk_end + timedelta(seconds=1)

        logger.info(f"✓ Total resolved incidents retrieved: {len(all_incidents)}")
        return all_incidents


class MTTRAnalyzer:
    """Analyzes incidents to calculate MTTR metrics."""

    @staticmethod
    def format_time(seconds: float) -> str:
        days = int(seconds // (24 * 3600))
        remaining = seconds % (24 * 3600)
        hours = int(remaining // 3600)
        minutes = int((remaining % 3600) // 60)
        secs = int(remaining % 60)

        if days > 0:
            return f"{days}d {hours}h {minutes}m {secs}s"
        return f"{hours}h {minutes}m {secs}s"

    @classmethod
    def calculate_metrics(cls, incidents: List[Dict]) -> Dict:
        if not incidents:
            return {
                "total_incidents": 0,
                "mttr_formatted": "0h 0m 0s",
                "mttr_hours": 0.0,
                "mttr_minutes": 0.0,
                "mttr_seconds": 0.0,
                "min_resolution_time_formatted": "0h 0m 0s",
                "max_resolution_time_formatted": "0h 0m 0s",
            }

        resolution_times = []
        for incident in incidents:
            created_str = incident.get("created_at")
            resolved_str = incident.get("resolved_at")
            if not created_str or not resolved_str:
                continue

            created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            resolved_at = datetime.fromisoformat(resolved_str.replace("Z", "+00:00"))
            resolution_times.append((resolved_at - created_at).total_seconds())

        if not resolution_times:
            return cls.calculate_metrics([])

        mttr_seconds = statistics.mean(resolution_times)

        return {
            "total_incidents": len(resolution_times),
            "mttr_formatted": cls.format_time(mttr_seconds),
            "mttr_hours": round(mttr_seconds / 3600, 2),
            "mttr_minutes": round(mttr_seconds / 60, 2),
            "mttr_seconds": round(mttr_seconds, 2),
            "min_resolution_time_formatted": cls.format_time(min(resolution_times)),
            "max_resolution_time_formatted": cls.format_time(max(resolution_times)),
        }

    @classmethod
    def analyze_by_service(cls, incidents: List[Dict]) -> Dict[str, Dict]:
        service_incidents = {}
        for incident in incidents:
            service = incident.get("service", {})
            service_name = service.get("summary", "Unknown Service")

            if service_name not in service_incidents:
                service_incidents[service_name] = []
            service_incidents[service_name].append(incident)

        results = {}
        for service_name, data in service_incidents.items():
            results[service_name] = cls.calculate_metrics(data)

        results["Overall Pipeline"] = cls.calculate_metrics(incidents)
        return results


def format_period_datetime(dt_str: str, time_zone: Optional[str] = None) -> str:
    """Formats a period boundary string. Unlike API response timestamps, these are
    constructed locally and carry no offset of their own, so the tz actually applied
    to the query (or UTC, if none was given) is attached here for display."""
    if not dt_str:
        return dt_str
    try:
        cleaned = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned) if "T" in cleaned else datetime.strptime(cleaned, "%Y-%m-%d")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=parse_timezone(time_zone) if time_zone else timezone.utc)

        formatted = dt.strftime("%Y-%m-%d %H:%M:%S %z")
        if len(formatted) > 5 and formatted[-5] in ("+", "-"):
            formatted = formatted[:-2] + ":" + formatted[-2:]
        return formatted.strip()
    except Exception:
        return dt_str.replace("T", " ")


def export_to_csv(
    mttr_stats: Dict[str, Dict],
    since: str,
    until: str,
    time_zone: Optional[str] = None,
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_services_mttr"
) -> str:
    """Exports structured MTTR statistics to a safely versioned timestamped CSV."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    headers = [
        "Service",
        "Total Incidents",
        "MTTR",
        "MTTR (Hours)",
        "MTTR (Minutes)",
        "MTTR (Seconds)",
        "Min Resolution Time",
        "Max Resolution Time",
    ]

    rows = []
    
    for service_name, stats in sorted(
        mttr_stats.items(), 
        key=lambda x: (1, "") if x[0] == "Overall Pipeline" else (0, x[0].lower())
    ):
        rows.append(
            {
                "Service": service_name,
                "Total Incidents": stats["total_incidents"],
                "MTTR": stats["mttr_formatted"],
                "MTTR (Hours)": stats["mttr_hours"],
                "MTTR (Minutes)": stats["mttr_minutes"],
                "MTTR (Seconds)": stats["mttr_seconds"],
                "Min Resolution Time": stats["min_resolution_time_formatted"],
                "Max Resolution Time": stats["max_resolution_time_formatted"],
            }
        )

    display_since = format_period_datetime(since, time_zone)
    display_until = format_period_datetime(until, time_zone)

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Time Period Start", display_since])
        writer.writerow(["Time Period End", display_until])
        writer.writerow([]) 

        dict_writer = csv.DictWriter(csvfile, fieldnames=headers)
        dict_writer.writeheader()
        dict_writer.writerows(rows)

    logger.info(f"✓ CSV output saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options with explicit default, relative lookback, and custom timezone options."""
    parser = argparse.ArgumentParser(
        description=f"PagerDuty MTTR Analyzer v{__version__}",
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
        default=None,
        metavar="TZ",
        help="Custom timezone IANA name (e.g., 'America/Santiago', 'UTC'). If omitted, dates are "
        "interpreted using the account's default time zone and output timestamps are rendered "
        "in UTC without an offset",
    )
    parser.add_argument(
        "-S",
        "--services",
        type=str,
        nargs="+",
        help="Space-separated list of Service Names (in quotes if they contain spaces) OR Service IDs to filter by",
    )
    parser.add_argument("-o", "--output", help="Custom CSV filename prefix")
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
        api = PagerDutyAPI(api_token)
        if not api.validate_token():
            sys.exit(1)

        start_time = time.time()

        resolved_service_ids = None
        if args.services:
            resolved_service_ids = api.resolve_service_identifiers(args.services)
            if not resolved_service_ids:
                logger.error("No valid Service IDs could be resolved from the provided names. Exiting.")
                sys.exit(1)

        incidents = api.fetch_resolved_incidents(
            since=since, until=until, service_ids=resolved_service_ids, time_zone=args.timezone
        )
        
        if not incidents:
            logger.info("No resolved incidents found for the given criteria.")
        else:
            logger.info("Calculating MTTR statistics...")
            mttr_stats = MTTRAnalyzer.analyze_by_service(incidents)
            output_filename = export_to_csv(mttr_stats, since, until, time_zone=args.timezone, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"Time Window ({args.timezone or 'account default'}): {since or 'Beginning'} -> {until or 'Now'}")
        print(f"✓ Analyzed MTTR across {len(incidents)} records in {elapsed:.2f}s")
        if incidents:
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