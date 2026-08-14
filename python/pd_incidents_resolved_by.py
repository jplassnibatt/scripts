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
    """PagerDuty REST API v2 client with user caching, rate limiting, and session management."""

    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.user_cache: Dict[str, Dict[str, str]] = {}
        self.min_interval = 1.0 / rate_limit
        self.last_request = 0.0
        self.rate_lock = threading.Lock()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-ResolvedIncidentsExporter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces thread-safe client-side rate limiting ($Rate = 8\\text{ req/s}$)."""
        with self.rate_lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request = time.time()

    def _request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes an HTTP GET request with retry backoff and rate-limit handling."""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited ($Rate = {self.min_interval:.3f}\\text{{s/req}}$). Waiting {wait}s..."
                    )
                    time.sleep(wait)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN permissions."
                    )
                    sys.exit(1)

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Timeout encountered (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error("Request timed out after maximum retries.")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Request failed after {max_retries} attempts: {e}")
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

    def get_user_details(self, user_id: str) -> Optional[Dict[str, str]]:
        """Fetch user details with local memory caching to eliminate duplicate API requests."""
        if user_id in self.user_cache:
            return self.user_cache[user_id]

        response = self._request(f"{self.base_url}/users/{user_id}")
        if response and response.status_code == 200:
            user_data = response.json().get("user", {})
            self.user_cache[user_id] = {
                "id": user_data.get("id", ""),
                "name": user_data.get("name", "Unknown"),
                "email": user_data.get("email", "N/A"),
            }
            return self.user_cache[user_id]
        return None

    def fetch_resolved_incidents_raw(
        self, since: Optional[str] = None, until: Optional[str] = None, service_ids: Optional[List[str]] = None, time_zone: Optional[str] = None
    ) -> List[Dict]:
        """Fetch resolved incidents (pagination only), natively evaluated by PagerDuty's time_zone
        handler when provided; otherwise the account's default time zone governs interpretation."""
        logger.info(f"Fetching resolved incidents from native API window: {since or 'Beginning'} -> {until or 'Now'} (TZ: {time_zone or 'account default'})")
        if service_ids:
            logger.info(f"Filtering by service IDs: {', '.join(service_ids)}")

        incidents = []
        offset = 0
        limit = 100

        while True:
            params = {
                "statuses[]": "resolved",
                "offset": offset,
                "limit": limit,
                "include[]": ["users"],
            }
            if time_zone:
                params["time_zone"] = time_zone

            if since:
                params["since"] = since
            if until:
                params["until"] = until
            if service_ids:
                params["service_ids[]"] = service_ids

            response = self._request(f"{self.base_url}/incidents", params=params)
            if not response:
                logger.error("Failed to fetch incident batch.")
                break

            data = response.json()
            incidents.extend(data.get("incidents", []))

            if not data.get("more", False):
                break

            offset += limit

        logger.info(f"✓ Retrieved {len(incidents)} resolved incidents")
        return incidents

    def enrich_incident_with_resolver(self, incident: Dict, time_zone: Optional[str] = None) -> Dict:
        """Fetch log entries for a single incident and identify who resolved it."""
        resolver = None
        resolver_details = None

        log_params = {"include[]": ["users"], "is_overview": "true"}
        if time_zone:
            log_params["time_zone"] = time_zone
        log_response = self._request(
            f"{self.base_url}/incidents/{incident['id']}/log_entries",
            params=log_params,
        )

        if log_response and log_response.status_code == 200:
            log_data = log_response.json()
            for entry in log_data.get("log_entries", []):
                if entry.get("type") == "resolve_log_entry":
                    resolver = entry.get("agent", {})
                    if resolver and resolver.get("id"):
                        resolver_details = self.get_user_details(resolver["id"])
                    break

        return {
            "incident_id": incident.get("id", "N/A"),
            "incident_number": incident.get("incident_number", "N/A"),
            "title": incident.get("title", "N/A"),
            "created_at": incident.get("created_at", "N/A"),
            "resolved_at": incident.get("resolved_at", "N/A"),
            "resolver": (
                {
                    "id": resolver.get("id") if resolver else None,
                    "name": (
                        resolver_details["name"]
                        if resolver_details
                        else resolver.get("summary") if resolver else "Unknown"
                    ),
                    "email": (
                        resolver_details["email"]
                        if resolver_details
                        else "Unknown"
                    ),
                }
                if resolver
                else None
            ),
            "urgency": incident.get("urgency", "N/A"),
            "service": incident.get("service", {}).get("summary", "N/A"),
            "service_id": incident.get("service", {}).get("id", "N/A"),
        }

    def get_resolved_incidents(
        self,
        since: Optional[str] = None,
        until: Optional[str] = None,
        service_ids: Optional[List[str]] = None,
        time_zone: Optional[str] = None,
        max_workers: int = 5,
    ) -> List[Dict]:
        """Fetch resolved incidents and enrich each with resolver details concurrently."""
        raw_incidents = self.fetch_resolved_incidents_raw(since, until, service_ids, time_zone)
        if not raw_incidents:
            return []

        logger.info(f"Resolving resolver details for {len(raw_incidents)} incidents using {max_workers} workers...")
        incidents = []
        completed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_incident = {
                executor.submit(self.enrich_incident_with_resolver, incident, time_zone): incident
                for incident in raw_incidents
            }

            for future in as_completed(future_to_incident):
                completed += 1
                try:
                    incidents.append(future.result())
                except Exception as e:
                    incident = future_to_incident[future]
                    logger.error(f"Error enriching incident {incident.get('id')}: {e}")

                if completed % 10 == 0 or completed == len(raw_incidents):
                    logger.info(f"Processed {completed}/{len(raw_incidents)} resolved incidents...")

        incidents.sort(key=lambda inc: inc.get("created_at") or "")

        logger.info(f"✓ Found total of {len(incidents)} resolved incidents")
        return incidents


def format_datetime(dt_str: str) -> str:
    """Reformats an ISO 8601 timestamp for display: 'T' becomes a space, and a missing
    offset (naive or 'Z') is made explicit as '+00:00' (UTC). The offset, if any, is
    taken as-is from the API response with no conversion applied."""
    if not dt_str or dt_str == "N/A":
        return dt_str
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
    incidents: List[Dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_incidents_resolved_by"
) -> Optional[str]:
    """Export resolved incident records to a safely versioned timestamped CSV file."""
    if not incidents:
        logger.info("No resolved incidents available to export.")
        return None

    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = [
        "Incident Number",
        "Incident ID",
        "Incident Title",
        "Created At",
        "Resolved At",
        "Resolver Name",
        "Resolver Email",
        "Resolver ID",
        "Service",
        "Service ID",
        "Urgency",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for incident in incidents:
            resolver = incident.get("resolver") or {}
            writer.writerow(
                {
                    "Incident Number": incident.get("incident_number", "N/A"),
                    "Incident ID": incident.get("incident_id", "N/A"),
                    "Incident Title": incident.get("title", "N/A"),
                    "Created At": format_datetime(incident.get("created_at", "")),
                    "Resolved At": format_datetime(incident.get("resolved_at", "")),
                    "Resolver Name": resolver.get("name", "Unknown"),
                    "Resolver Email": resolver.get("email", "Unknown"),
                    "Resolver ID": resolver.get("id", "Unknown"),
                    "Service": incident.get("service", "N/A"),
                    "Service ID": incident.get("service_id", "N/A"),
                    "Urgency": incident.get("urgency", "N/A"),
                }
            )

    logger.info(f"✓ CSV report saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser options."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Incidents Resolved By v{__version__}",
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
        help="Custom timezone IANA name (e.g., 'America/Santiago', 'UTC'). If omitted, dates are "
        "interpreted using the account's default time zone and output timestamps are rendered "
        "in UTC without an offset",
    )
    parser.add_argument(
        "--service-id",
        action="append",
        dest="service_ids",
        help="Service ID to filter (repeatable)",
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_incidents_resolved_by", help="Custom CSV filename prefix"
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=8,
        help="Maximum API requests per second",
    )
    parser.add_argument(
        "-w",
        "--max-workers",
        type=int,
        default=5,
        help="Maximum concurrent worker threads for resolver lookups",
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

    service_ids = args.service_ids or []
    if not service_ids:
        env_services = os.environ.get("SERVICE_IDS", "").strip()
        if env_services:
            service_ids = [s.strip() for s in env_services.split(",") if s.strip()]

    try:
        api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        if not api.validate_token():
            sys.exit(1)

        start_time = time.time()

        incidents = api.get_resolved_incidents(
            since=since,
            until=until,
            service_ids=service_ids,
            time_zone=args.timezone,
            max_workers=args.max_workers,
        )
        if not incidents:
            logger.warning("No resolved incidents found matching the criteria.")
            sys.exit(0)

        output_filename = export_to_csv(incidents, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"Time Window ({args.timezone or 'account default'}): {since or 'Beginning'} -> {until or 'Now'}")
        print(f"✓ Processed {len(incidents)} resolved incidents in {elapsed:.2f}s")
        print(f"✓ Output file: {output_filename or 'N/A'}")
        print(f"{'='*60}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()