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
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Dict, List, Optional
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


@dataclass
class PriorityChange:
    incident_id: str
    old_priority: str
    new_priority: str
    user_id: str
    changed_by: str
    timestamp: str
    incident_summary: str
    acknowledgers: str
    incident_url: str
    de_escalation: str
    mtta: str
    mttr: str


class PagerDutyAPI:
    """PagerDuty REST API v2 Client with thread-safe rate limiting."""

    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token:
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.min_interval = 1.0 / rate_limit
        self.last_request = 0.0
        self.lock = threading.Lock()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-PriorityExporter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces thread-safe client-side rate limiting ($Rate = 8\\text{ req/s}$)."""
        with self.lock:
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

    def fetch_paginated_data(
        self, url: str, key: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Generic pagination fetcher checking offset/more metadata."""
        all_data = []
        offset = 0
        limit = 100
        params = params or {}

        while True:
            params["offset"] = offset
            params["limit"] = limit

            response = self._request(url, params=params)
            if not response:
                break

            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON response: {e}")
                break

            items = data.get(key, [])
            all_data.extend(items)

            if not data.get("more"):
                break

            offset += limit

        return all_data

    def fetch_resolved_incidents(
        self, since: Optional[str] = None, until: Optional[str] = None, time_zone: str = "UTC"
    ) -> List[Dict[str, Any]]:
        """Fetches resolved incidents using natively evaluated timezone windows."""
        logger.info(
            f"Fetching resolved incidents: {since or 'Beginning'} -> {until or 'Now'} (TZ: {time_zone})"
        )
        params = {"statuses[]": "resolved", "time_zone": time_zone}
        if since:
            params["since"] = since
        if until:
            params["until"] = until

        incidents = self.fetch_paginated_data(
            f"{self.base_url}/incidents", key="incidents", params=params
        )
        logger.info(f"✓ Retrieved {len(incidents)} resolved incidents")
        return incidents

    def fetch_log_entries(self, incident_id: str, time_zone: str = "UTC") -> List[Dict[str, Any]]:
        """Fetches log entries for a single incident natively offset to target timezone."""
        return self.fetch_paginated_data(
            f"{self.base_url}/incidents/{incident_id}/log_entries",
            key="log_entries",
            params={"time_zone": time_zone}
        )


class PriorityAnalyzer:
    """Analyzes log entries for incident priority shifts and metrics."""

    @staticmethod
    def safe_get(dictionary: Dict[str, Any], *keys: str) -> Any:
        for key in keys:
            dictionary = dictionary.get(key, {})
            if dictionary is None:
                return None
        return dictionary if dictionary != {} else None

    @staticmethod
    def find_acknowledgers(log_entries: List[Dict[str, Any]]) -> str:
        acknowledgers = [
            PriorityAnalyzer.safe_get(entry, "agent", "summary")
            for entry in log_entries
            if entry.get("type") == "acknowledge_log_entry"
        ]
        return ", ".join(filter(None, acknowledgers)) or "Not acknowledged"

    @staticmethod
    def calculate_time_difference(start: datetime, end: datetime) -> str:
        delta = end - start
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:
            parts.append(f"{seconds}s")

        return " ".join(parts)

    @staticmethod
    def is_de_escalation(old_priority: str, new_priority: str) -> bool:
        priority_order = {"P1": 1, "P2": 2, "P3": 3, "P4": 4, "P5": 5}
        if old_priority in priority_order and new_priority in priority_order:
            return priority_order[new_priority] > priority_order[old_priority]
        return False

    @staticmethod
    def calculate_mtta(
        log_entries: List[Dict[str, Any]], created_at: datetime
    ) -> str:
        for entry in log_entries:
            if entry.get("type") == "acknowledge_log_entry":
                ack_str = entry.get("created_at")
                if ack_str:
                    ack_dt = datetime.fromisoformat(
                        ack_str.replace("Z", "+00:00")
                    )
                    return PriorityAnalyzer.calculate_time_difference(
                        created_at, ack_dt
                    )
        return "Not acknowledged"

    @staticmethod
    def calculate_mttr(incident: Dict[str, Any]) -> str:
        created_str = incident.get("created_at")
        resolved_str = incident.get("resolved_at")
        if not created_str or not resolved_str:
            return "N/A"

        created_at = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        resolved_at = datetime.fromisoformat(
            resolved_str.replace("Z", "+00:00")
        )
        return PriorityAnalyzer.calculate_time_difference(
            created_at, resolved_at
        )

    @classmethod
    def process_incident(
        cls, api: PagerDutyAPI, incident: Dict[str, Any], time_zone: str = "UTC"
    ) -> List[PriorityChange]:
        target_tz = parse_timezone(time_zone)
        log_entries = sorted(
            api.fetch_log_entries(incident["id"], time_zone=time_zone),
            key=lambda x: x.get("created_at", ""),
        )

        acknowledgers = cls.find_acknowledgers(log_entries)
        incident_url = (
            cls.safe_get(log_entries[0], "incident", "html_url")
            if log_entries
            else None
        )
        
        created_str = incident.get("created_at")
        created_at = (
            datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if created_str
            else datetime.now(timezone.utc)
        )

        mtta = cls.calculate_mtta(log_entries, created_at)
        mttr = cls.calculate_mttr(incident)

        priority_changes = []
        for entry in reversed(log_entries):
            if entry.get("type") == "priority_change_log_entry":
                old_priority = cls.safe_get(
                    entry, "channel", "old_priority", "name"
                )
                new_priority = cls.safe_get(
                    entry, "channel", "new_priority", "name"
                )

                if old_priority and new_priority:
                    incident_id = cls.safe_get(entry, "incident", "id") or incident.get("id")
                    user_id = cls.safe_get(entry, "agent", "id") or "N/A"
                    de_escalation = (
                        "Yes"
                        if cls.is_de_escalation(old_priority, new_priority)
                        else "No"
                    )
                    
                    created_at_raw = entry.get("created_at", "")
                    if created_at_raw:
                        try:
                            dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=target_tz)
                            else:
                                dt = dt.astimezone(target_tz)

                            formatted = dt.strftime("%Y-%m-%d %H:%M:%S %z")
                            if len(formatted) > 5 and formatted[-5] in ('+', '-'):
                                formatted = formatted[:-2] + ":" + formatted[-2:]
                            created_at_local = formatted
                        except Exception:
                            created_at_local = created_at_raw
                    else:
                        created_at_local = ""

                    priority_changes.append(
                        PriorityChange(
                            incident_id=incident_id,
                            old_priority=old_priority,
                            new_priority=new_priority,
                            user_id=user_id,
                            changed_by=cls.safe_get(entry, "agent", "summary") or "System",
                            timestamp=created_at_local,
                            incident_summary=cls.safe_get(entry, "incident", "summary") or incident.get("summary", ""),
                            acknowledgers=acknowledgers,
                            incident_url=incident_url or f"https://pagerduty.com/incidents/{incident_id}",
                            de_escalation=de_escalation,
                            mtta=mtta,
                            mttr=mttr,
                        )
                    )

        return priority_changes


def process_incidents_concurrently(
    api: PagerDutyAPI, incidents: List[Dict[str, Any]], time_zone: str = "UTC", max_workers: int = 5
) -> List[PriorityChange]:
    """Processes incidents concurrently to fetch log entries efficiently."""
    changes = []
    total = len(incidents)
    if total == 0:
        return changes

    logger.info(f"Analyzing priority changes across {total} incidents...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_incident = {
            executor.submit(PriorityAnalyzer.process_incident, api, incident, time_zone): incident
            for incident in incidents
        }

        completed = 0
        for future in as_completed(future_to_incident):
            try:
                changes.extend(future.result())
            except Exception as e:
                logger.error(f"Error processing incident: {e}")
            completed += 1
            if completed % 10 == 0 or completed == total:
                logger.info(f"Processed {completed}/{total} incidents...")

    return changes


def export_to_csv(
    priority_changes: List[PriorityChange],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_priority_changes",
) -> str:
    """Exports list of PriorityChange objects to a safely versioned timestamped CSV format."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = [
        "Incident ID",
        "Old Priority",
        "New Priority",
        "User ID",
        "Changed By",
        "Time of Change",
        "Incident Summary",
        "Acknowledgers",
        "Incident URL",
        "De-escalation",
        "MTTA",
        "MTTR",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if priority_changes:
            for change in priority_changes:
                row = asdict(change)
                mapped_row = {
                    "Incident ID": row.get("incident_id"),
                    "Old Priority": row.get("old_priority"),
                    "New Priority": row.get("new_priority"),
                    "User ID": row.get("user_id"),
                    "Changed By": row.get("changed_by"),
                    "Time of Change": row.get("timestamp"),
                    "Incident Summary": row.get("incident_summary"),
                    "Acknowledgers": row.get("acknowledgers"),
                    "Incident URL": row.get("incident_url"),
                    "De-escalation": row.get("de_escalation"),
                    "MTTA": row.get("mtta"),
                    "MTTR": row.get("mttr"),
                }
                writer.writerow(mapped_row)

    logger.info(f"✓ CSV output saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options with explicit default, relative lookback, and custom timezone options."""
    parser = argparse.ArgumentParser(
        description=f"PagerDuty Priority Changes Exporter v{__version__}",
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
        default="UTC",
        metavar="TZ",
        help="Custom timezone IANA name for relative calendar calculations (e.g., 'America/Santiago', 'UTC')",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_priority_changes",
        help="Custom CSV filename prefix",
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=8,
        help="API limit rate in req/s (default: 8)",
    )
    return parser


def main() -> None:
    parser = build_parser()

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
    now_local = datetime.now(target_tz)

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

        resolved_incidents = api.fetch_resolved_incidents(since=since, until=until, time_zone=args.timezone)
        
        priority_changes = process_incidents_concurrently(api, resolved_incidents, time_zone=args.timezone)

        priority_changes.sort(key=lambda x: x.timestamp, reverse=True)

        output_filename = export_to_csv(priority_changes, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"✓ Processed {len(priority_changes)} priority change records in {elapsed:.2f}s")
        print(f"✓ Report saved to '{output_filename or 'N/A'}'")
        print(f"{'='*50}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()