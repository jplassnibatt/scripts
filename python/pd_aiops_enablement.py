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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter

__version__ = "1.3.3"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class PagerDutyAIOpsReporter:
    """Optimized PagerDuty API v2 client for bulk AIOps status auditing."""

    def __init__(
        self,
        api_token: str,
        subdomain: Optional[str] = None,
        max_workers: int = 15,
    ):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty.")

        self.api_token = api_token.strip()
        self.base_url = "https://api.pagerduty.com"
        self.subdomain = subdomain
        self.max_workers = max_workers

        self.session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=max_workers, pool_maxsize=max_workers * 2
        )
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={self.api_token}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-AIOpsReporter/{__version__}",
            }
        )

        self.rate_limit_delay = 0.04
        self.request_lock = threading.Lock()

    def _make_request(self, url: str, max_retries: int = 5) -> Dict[str, Any]:
        """Executes HTTP GET with thread-safe rate limiting and bounded 429 retry handling."""
        for attempt in range(max_retries):
            with self.request_lock:
                time.sleep(self.rate_limit_delay)

            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited (429). Retrying after {retry_after}s... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for URL {url}: {e}")
                return {}

        logger.error(f"Giving up on {url} after {max_retries} rate-limit retries.")
        return {}

    def validate_token(self) -> bool:
        """Validates API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        data = self._make_request(f"{self.base_url}/users?limit=1")
        if data:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def extract_subdomain_from_url(self, html_url: str) -> str:
        """Extracts PagerDuty tenant subdomain from resource HTML URL."""
        if not html_url:
            return ""
        match = re.match(r"https://([^.]+)\.pagerduty\.com/", html_url)
        return match.group(1) if match else ""

    def set_subdomain_from_services(
        self, services: List[Dict[str, Any]]
    ) -> None:
        """Auto-detects tenant subdomain from the first available service URL."""
        if self.subdomain:
            return
        for service in services:
            html_url = service.get("html_url", "")
            subdomain = self.extract_subdomain_from_url(html_url)
            if subdomain:
                self.subdomain = subdomain
                logger.info(
                    f"Auto-detected PagerDuty subdomain: {self.subdomain}"
                )
                return
        logger.warning("Could not auto-detect subdomain from service URLs.")

    def get_all_items_paginated(
        self, endpoint: str, item_key: str
    ) -> List[Dict[str, Any]]:
        """Generic offset-paginated resource retrieval loop."""
        items = []
        offset = 0
        limit = 100

        while True:
            url = f"{self.base_url}/{endpoint}?limit={limit}&offset={offset}"
            data = self._make_request(url)

            if not data or item_key not in data:
                break

            batch = data[item_key]
            items.extend(batch)

            if not data.get("more", False):
                break

            offset += limit

        logger.info(f"Fetched {len(items)} {item_key}.")
        return items

    def get_all_services(self) -> List[Dict[str, Any]]:
        return self.get_all_items_paginated("services", "services")

    def get_all_orchestrations(self) -> List[Dict[str, Any]]:
        return self.get_all_items_paginated(
            "event_orchestrations", "orchestrations"
        )

    def get_aiops_status(self, item_id: str, item_type: str) -> bool:
        """Retrieves AIOps enablement feature flag for a target resource."""
        endpoint = (
            "services" if item_type == "service" else "event_orchestrations"
        )
        url = f"{self.base_url}/{endpoint}/{item_id}/enablements"
        data = self._make_request(url)

        if not data or "enablements" not in data:
            return False

        for enablement in data["enablements"]:
            if enablement.get("feature") == "aiops":
                return enablement.get("enabled", False)

        return False

    def process_items_batch(
        self, items: List[Tuple[str, str, Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Processes AIOps enablement status concurrently via ThreadPoolExecutor."""
        results = []

        def worker(item_data):
            item_id, item_type, item_info = item_data
            try:
                enabled = self.get_aiops_status(item_id, item_type)
                return (item_id, item_type, enabled, item_info)
            except Exception as e:
                logger.error(
                    f"Error fetching AIOps status for {item_type} {item_id}: {e}"
                )
                return (item_id, item_type, False, item_info)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {
                executor.submit(worker, item): item for item in items
            }
            for future in as_completed(future_to_item):
                results.append(future.result())

        return results

    def generate_orchestration_url(self, orchestration_id: str) -> str:
        """Constructs direct web link for event orchestrations."""
        sub = self.subdomain or "your-subdomain"
        return f"https://{sub}.pagerduty.com/event-orchestration/{orchestration_id}"

    def fetch_audit_data(self) -> List[Dict[str, Any]]:
        """Runs end-to-end audit and returns flattened rows for CSV export."""
        logger.info(
            f"Starting PagerDuty AIOps report generation (v{__version__})..."
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            s_future = executor.submit(self.get_all_services)
            o_future = executor.submit(self.get_all_orchestrations)
            services = s_future.result()
            orchestrations = o_future.result()

        self.set_subdomain_from_services(services)

        items_to_process = [(s["id"], "service", s) for s in services] + [
            (o["id"], "event_orchestration", o) for o in orchestrations
        ]

        logger.info(
            f"Auditing AIOps enablement across {len(items_to_process)} items using {self.max_workers} worker threads..."
        )
        processed_results = self.process_items_batch(items_to_process)

        csv_rows = []
        for item_id, item_type, aiops_enabled, item_info in processed_results:
            html_url = (
                item_info.get("html_url", "")
                if item_type == "service"
                else self.generate_orchestration_url(item_id)
            )

            csv_rows.append(
                {
                    "Type": item_type,
                    "ID": item_id,
                    "Name": item_info.get("name", "Unknown"),
                    "AIOps Enabled": aiops_enabled,
                    "HTML URL": html_url,
                }
            )

        type_order = {"service": 0, "event_orchestration": 1}
        csv_rows.sort(
            key=lambda r: (type_order.get(r["Type"], 2), r["Name"].lower())
        )

        return csv_rows


def export_to_csv(
    data: List[Dict[str, Any]],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_aiops",
) -> str:
    """Exports structured log data to a safely versioned timestamped CSV file."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = ["Type", "ID", "Name", "AIOps Enabled", "HTML URL"]
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        if data:
            writer.writerows(data)

    logger.info(f"✓ CSV report generated: {filename}")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Audit AIOps enablement v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_aiops", help="Custom CSV filename prefix"
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=15,
        help="Number of concurrent worker threads",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get(
        "API_TOKEN"
    )
    if not api_token or api_token.strip() == "YOUR_API_TOKEN_HERE":
        logger.error(
            "ERROR: Missing API token. Export PAGERDUTY_API_TOKEN environment variable."
        )
        sys.exit(1)

    try:
        reporter = PagerDutyAIOpsReporter(api_token, max_workers=args.workers)
        if not reporter.validate_token():
            sys.exit(1)

        start_time = time.time()

        audit_data = reporter.fetch_audit_data()
        
        if not audit_data:
            logger.warning("No services or orchestrations found.")
            sys.exit(0)

        output_file = export_to_csv(audit_data, prefix=args.output)
        
        elapsed = time.time() - start_time
        services_enabled = sum(1 for r in audit_data if r["Type"] == "service" and r["AIOps Enabled"])
        orchestrations_enabled = sum(1 for r in audit_data if r["Type"] == "event_orchestration" and r["AIOps Enabled"])

        print(f"\n{'='*60}")
        print(f"✓ Audited {len(audit_data)} total items in {elapsed:.2f}s")
        print(f"✓ Services: {services_enabled} AIOps enabled")
        print(f"✓ Orchestrations: {orchestrations_enabled} AIOps enabled")
        print(f"✓ Output file: {output_file}")
        print(f"{'='*60}\n")
        
    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()