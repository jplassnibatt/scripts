#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Dict, List, Optional
import requests

__version__ = "1.2.2"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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

def format_datetime(dt_str: Optional[str]) -> str:
    """Formats ISO datetime string from natively localized API responses with a colonized offset."""
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        
        # Native %z produces '+0000' or '-0400'. We slice the string to inject the colon.
        formatted = dt.strftime("%Y-%m-%d %H:%M:%S %z")
        if len(formatted) > 5 and formatted[-5] in ('+', '-'):
            formatted = formatted[:-2] + ":" + formatted[-2:]
            
        return formatted
    except Exception:
        return dt_str

class PagerDutyAcknowledgeExporter:
    """PagerDuty REST API v2 client for retrieving incident acknowledgment logs."""

    def __init__(self, api_token: str):
        if not api_token or api_token.strip() in ("", "YOUR_API_TOKEN"):
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-AckExporter/{__version__}",
            }
        )

    def _request(
        self, url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Executes HTTP GET requests with exponential backoff and rate limit handling."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limit hit ($Rate = 250\\text{{ req/min}}$). Sleeping for {wait}s..."
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

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Request failed after {max_retries} attempts: {e}")
                    return None
        return None

    def fetch_acknowledgments(self, incident_id: str, time_zone: str = "UTC") -> List[Dict[str, Any]]:
        """Fetches all acknowledgment log entries natively offset to target timezone."""
        logger.info(f"Fetching log entries for Incident ID: {incident_id} (TZ: {time_zone})...")

        url = f"{self.base_url}/incidents/{incident_id}/log_entries"
        offset = 0
        limit = 100
        acknowledgments = []

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "include[]": ["users", "channels"],
                "time_zone": time_zone,  # Pass timezone natively
            }

            response = self._request(url, params=params)
            if not response:
                break

            data = response.json()
            log_entries = data.get("log_entries", [])

            for entry in log_entries:
                if entry.get("type") == "acknowledge_log_entry":
                    agent = entry.get("agent") or {}
                    channel = entry.get("channel") or {}

                    ack_at_raw = entry.get("created_at")

                    acknowledgments.append(
                        {
                            "incident_id": incident_id,
                            "log_entry_id": entry.get("id"),
                            "acknowledged_at": format_datetime(ack_at_raw),
                            "acknowledged_by": agent.get("summary", "Unknown User"),
                            "agent_type": agent.get("type", "unknown"),
                            "user_id": agent.get("id", ""),
                            "channel_type": channel.get("type", "Unknown Channel"),
                        }
                    )

            if not data.get("more", False):
                break

            offset += limit

        logger.info(f"✓ Total acknowledgment entries found: {len(acknowledgments)}")
        return acknowledgments

def export_to_csv(
    data: List[Dict[str, Any]],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_incident_ack",
) -> str:
    """Exports structured log data to a safely versioned timestamped CSV file."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    # Use static, human-readable column names for stable downstream ingestion
    fieldnames = [
        "Incident ID",
        "Log Entry ID",
        "Acknowledged At",
        "Acknowledged By",
        "User ID",
        "Agent Type",
        "Channel Type",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        if data:
            for row in data:
                mapped_row = {
                    "Incident ID": row.get("incident_id"),
                    "Log Entry ID": row.get("log_entry_id"),
                    "Acknowledged At": row.get("acknowledged_at"),
                    "Acknowledged By": row.get("acknowledged_by"),
                    "User ID": row.get("user_id"),
                    "Agent Type": row.get("agent_type"),
                    "Channel Type": row.get("channel_type"),
                }
                writer.writerow(mapped_row)

    logger.info(f"✓ CSV report generated: {filename}")
    return filename

class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):

    """Custom help formatter providing extended spacing for flag alignment."""
    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)

def build_parser() -> argparse.ArgumentParser:
    """Builds command-line interface arguments."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Incident Acknowledgment v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-i",
        "--incident-id",
        required=True,
        help="The PagerDuty Incident ID to inspect (e.g., Q0FD2E8Z18VVGP)",
    )
    parser.add_argument(
        "-t",
        "--timezone",
        default="UTC",
        metavar="TZ",
        help="Custom timezone IANA name (e.g., 'America/Santiago', 'UTC')",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_incident_ack",
        help="Custom CSV filename prefix",
    )
    return parser

def main() -> None:
    parser = build_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get("API_TOKEN")
    if not api_token or api_token.strip() == "YOUR_API_TOKEN":
        logger.error(
            "ERROR: Missing API token. Export the PAGERDUTY_API_TOKEN environment variable."
        )
        sys.exit(1)

    target_tz = parse_timezone(args.timezone)

    try:
        exporter = PagerDutyAcknowledgeExporter(api_token)
        start_time = time.time()

        ack_entries = exporter.fetch_acknowledgments(args.incident_id, time_zone=args.timezone)
        
        # Call export_to_csv without the time_zone argument
        output_file = export_to_csv(ack_entries, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"✓ Incident ID: {args.incident_id}")
        print(f"✓ Acknowledgments Extracted: {len(ack_entries)}")
        print(f"✓ Execution Time: {elapsed:.2f}s")
        print(f"✓ Output File: {output_file}")
        print(f"{'='*50}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()