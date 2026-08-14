#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests

__version__ = "1.3.0"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

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

    def validate_token(self) -> bool:
        """Validates API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._request(f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def fetch_acknowledgments(
        self,
        incident_id: str,
        time_zone: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetches all acknowledgment log entries, natively offset to the target timezone
        when provided; otherwise the account's default time zone governs interpretation."""
        logger.info(f"Fetching log entries for Incident ID: {incident_id} (TZ: {time_zone or 'account default'})...")

        url = f"{self.base_url}/incidents/{incident_id}/log_entries"
        offset = 0
        limit = 100
        acknowledgments = []

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "include[]": ["users", "channels"],
            }
            if time_zone:
                params["time_zone"] = time_zone

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
    default_prefix: str = "pagerduty_incidents_ack",
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
        default=None,
        metavar="TZ",
        help="Custom timezone IANA name (e.g., 'America/Santiago', 'UTC'). If omitted, dates are "
        "interpreted using the account's default time zone and output timestamps are rendered "
        "in UTC without an offset",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_incidents_ack",
        help="Custom CSV filename prefix",
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
    if not api_token or api_token.strip() == "YOUR_API_TOKEN":
        logger.error(
            "Set it using: export PAGERDUTY_API_TOKEN='your-api-token-here'"
        )
        sys.exit(1)

    try:
        exporter = PagerDutyAcknowledgeExporter(api_token)
        if not exporter.validate_token():
            sys.exit(1)

        start_time = time.time()

        ack_entries = exporter.fetch_acknowledgments(
            args.incident_id, time_zone=args.timezone
        )

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