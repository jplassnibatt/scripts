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
from typing import Any, Dict, List, Optional, Set, Tuple
import requests
from requests.adapters import HTTPAdapter

__version__ = "1.4.0"

logging.basicConfig(
    level=logging.WARNING,
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

    def _make_write_request(
        self, method: str, url: str, json_data: Optional[Dict] = None, max_retries: int = 5
    ) -> Optional[requests.Response]:
        """Executes a thread-safe rate-limited write (PUT/PATCH/POST) request."""
        for attempt in range(max_retries):
            with self.request_lock:
                time.sleep(self.rate_limit_delay)

            try:
                response = self.session.request(method, url, json=json_data, timeout=30)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited (429). Retrying after {retry_after}s... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                return response
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for URL {url}: {e}")
                return None

        logger.error(f"Giving up on {url} after {max_retries} rate-limit retries.")
        return None

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

    def get_endpoint_for_type(self, item_type: str) -> str:
        """Maps an item type to its PagerDuty REST API v2 collection endpoint."""
        return "services" if item_type == "service" else "event_orchestrations"

    def get_aiops_status(self, item_id: str, item_type: str) -> bool:
        """Retrieves AIOps enablement feature flag for a target resource."""
        endpoint = self.get_endpoint_for_type(item_type)
        url = f"{self.base_url}/{endpoint}/{item_id}/enablements"
        data = self._make_request(url)

        if not data or "enablements" not in data:
            return False

        for enablement in data["enablements"]:
            if enablement.get("feature") == "aiops":
                return enablement.get("enabled", False)

        return False

    def set_aiops_status(
        self, item_id: str, item_type: str, enabled: bool, item_name: str = ""
    ) -> Dict[str, Any]:
        """Enables or disables the AIOps feature flag for a target resource."""
        previous_state = self.get_aiops_status(item_id, item_type)

        endpoint = self.get_endpoint_for_type(item_type)
        url = f"{self.base_url}/{endpoint}/{item_id}/enablements/aiops"
        body = {"enablement": {"enabled": enabled}}
        response = self._make_write_request("PUT", url, json_data=body)

        action = "enabled" if enabled else "disabled"
        if response is not None and response.status_code == 200:
            logger.info(f"✓ AIOps {action}: {item_name} ({item_type} ID: {item_id})")
            return {
                "id": item_id,
                "type": item_type,
                "name": item_name,
                "previous_state": previous_state,
                "current_state": enabled,
                "status": "Updated",
            }
        elif response is not None and response.status_code == 404:
            logger.warning(f"✗ Not found: {item_name} ({item_type} ID: {item_id})")
            return {
                "id": item_id,
                "type": item_type,
                "name": item_name,
                "previous_state": previous_state,
                "current_state": previous_state,
                "status": "Not Found",
            }
        else:
            status = response.status_code if response is not None else "No Response"
            logger.error(f"✗ Failed: {item_name} ({item_type} ID: {item_id}) - Status: {status}")
            return {
                "id": item_id,
                "type": item_type,
                "name": item_name,
                "previous_state": previous_state,
                "current_state": previous_state,
                "status": f"Failed ({status})",
            }

    def update_items_batch(
        self, items: List[Tuple[str, str, str]], enabled: bool
    ) -> List[Dict[str, Any]]:
        """Enables/disables AIOps concurrently across a batch of items via ThreadPoolExecutor."""
        results = []

        def worker(item_data):
            item_id, item_type, item_name = item_data
            try:
                return self.set_aiops_status(item_id, item_type, enabled, item_name)
            except Exception as e:
                logger.error(
                    f"Error updating AIOps status for {item_type} {item_id}: {e}"
                )
                return {
                    "id": item_id,
                    "type": item_type,
                    "name": item_name,
                    "previous_state": None,
                    "current_state": None,
                    "status": f"Failed (Exception: {e})",
                }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(worker, item) for item in items]
            for future in as_completed(futures):
                results.append(future.result())

        return results

    def fetch_items_list(self) -> List[Dict[str, str]]:
        """Fetches and flattens all services and orchestrations into a simple selectable list."""
        with ThreadPoolExecutor(max_workers=2) as executor:
            s_future = executor.submit(self.get_all_services)
            o_future = executor.submit(self.get_all_orchestrations)
            services = s_future.result()
            orchestrations = o_future.result()

        items = [
            {"id": s["id"], "type": "service", "name": s.get("name", "Unknown")}
            for s in services
        ] + [
            {"id": o["id"], "type": "event_orchestration", "name": o.get("name", "Unknown")}
            for o in orchestrations
        ]

        type_order = {"service": 0, "event_orchestration": 1}
        items.sort(key=lambda r: (type_order.get(r["type"], 2), r["name"].lower()))
        return items

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
    resolved_prefix = prefix or default_prefix

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


def read_items_from_csv(filename: str) -> List[Dict[str, str]]:
    """Reads item Type/ID/Name from a CSV input file (accepts the audit report format)."""
    valid_types = {"service", "event_orchestration"}
    items = []
    try:
        with open(filename, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                item_id = row.get("ID") or row.get("id")
                item_name = row.get("Name") or row.get("name") or ""
                item_type = (row.get("Type") or row.get("type") or "service").strip().lower()
                if item_type not in valid_types:
                    logger.warning(f"Skipping row with unrecognized Type '{item_type}' (ID: {item_id})")
                    continue
                if item_id:
                    items.append(
                        {"id": item_id.strip(), "type": item_type, "name": item_name.strip()}
                    )
        logger.info(f"✓ Loaded {len(items)} items from '{filename}'")
        return items
    except FileNotFoundError:
        logger.error(f"File '{filename}' not found.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        sys.exit(1)


def parse_selection_input(input_str: str, max_count: int) -> List[int]:
    """
    Parses comma-separated list numbers and range strings (e.g., '1, 3-5, 8, 10-12').
    Returns 0-based unique list indices.
    """
    selected_indices: Set[int] = set()
    parts = [p.strip() for p in input_str.split(",") if p.strip()]

    for part in parts:
        if "-" in part:
            subparts = part.split("-")
            if len(subparts) == 2 and subparts[0].isdigit() and subparts[1].isdigit():
                start, end = int(subparts[0]), int(subparts[1])
                for idx in range(min(start, end), max(start, end) + 1):
                    if 1 <= idx <= max_count:
                        selected_indices.add(idx - 1)
        elif part.isdigit():
            idx = int(part)
            if 1 <= idx <= max_count:
                selected_indices.add(idx - 1)

    return sorted(list(selected_indices))


def prompt_interactive_selection(all_items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Displays an interactive list of services/orchestrations and parses user selections."""
    if not all_items:
        logger.info("No services or orchestrations available to select.")
        return []

    print("\n" + "=" * 80)
    print("AVAILABLE PAGERDUTY SERVICES & EVENT ORCHESTRATIONS")
    print("=" * 80)
    for idx, item in enumerate(all_items, 1):
        type_label = "Service" if item["type"] == "service" else "Orchestration"
        print(f"[{idx:3d}] ({type_label}) {item['name']} (ID: {item['id']})")
    print("=" * 80)

    print("\nSelection options:")
    print("  - Single/Comma-separated numbers: e.g., 1, 3, 5")
    print("  - Range of numbers: e.g., 1-5")
    print("  - Combination: e.g., 1-3, 5, 8-10")

    user_input = input("\nEnter numbers to select (or 'cancel' to exit): ").strip()
    if user_input.lower() in ("cancel", "exit", "q", ""):
        logger.info("Interactive selection cancelled.")
        sys.exit(0)

    selected_indices = parse_selection_input(user_input, len(all_items))
    return [all_items[i] for i in selected_indices]


def export_update_report_csv(
    results: List[Dict[str, Any]],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_aiops_update",
) -> Optional[str]:
    """Exports enable/disable execution results to a safely versioned timestamped CSV file."""
    if not results:
        logger.info("No update results to export.")
        return None

    resolved_prefix = prefix or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = ["Type", "ID", "Name", "Previous State", "Current State", "Status"]
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(
                {
                    "Type": row.get("type"),
                    "ID": row.get("id"),
                    "Name": row.get("name"),
                    "Previous State": row.get("previous_state"),
                    "Current State": row.get("current_state"),
                    "Status": row.get("status"),
                }
            )

    logger.info(f"✓ Update execution report saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty AIOps Enablement Manager v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "-a",
        "--audit",
        action="store_true",
        help="Audit AIOps enablement status across all services and orchestrations",
    )
    mode_group.add_argument(
        "-e", "--enable", action="store_true", help="Enable AIOps for the selected items"
    )
    mode_group.add_argument(
        "-d", "--disable", action="store_true", help="Disable AIOps for the selected items"
    )

    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "-f",
        "--file",
        help="CSV input file containing items to enable/disable (columns: 'Type', 'ID', 'Name')",
    )
    input_group.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Interactively fetch and select services/orchestrations from PagerDuty to enable/disable",
    )

    parser.add_argument(
        "-o", "--output", default=None, help="Custom CSV output filename prefix"
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=15, help="Number of concurrent worker threads"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt and execute the enable/disable immediately",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed [INFO] level log messages",
    )

    return parser


def run_audit(reporter: PagerDutyAIOpsReporter, args: argparse.Namespace) -> None:
    start_time = time.time()

    audit_data = reporter.fetch_audit_data()

    if not audit_data:
        logger.warning("No services or orchestrations found.")
        sys.exit(0)

    output_file = export_to_csv(audit_data, prefix=args.output)

    elapsed = time.time() - start_time
    services_enabled = sum(1 for r in audit_data if r["Type"] == "service" and r["AIOps Enabled"])
    orchestrations_enabled = sum(
        1 for r in audit_data if r["Type"] == "event_orchestration" and r["AIOps Enabled"]
    )

    print(f"\n{'='*60}")
    print(f"✓ Audited {len(audit_data)} total items in {elapsed:.2f}s")
    print(f"✓ Services: {services_enabled} AIOps enabled")
    print(f"✓ Orchestrations: {orchestrations_enabled} AIOps enabled")
    print(f"✓ Output file: {output_file}")
    print(f"{'='*60}\n")


def run_update(reporter: PagerDutyAIOpsReporter, args: argparse.Namespace) -> None:
    enabled = bool(args.enable)
    action_word = "ENABLE" if enabled else "DISABLE"

    if args.file:
        targets = read_items_from_csv(args.file)
    else:
        all_items = reporter.fetch_items_list()
        targets = prompt_interactive_selection(all_items)

    if not targets:
        logger.info("No items selected for update. Exiting.")
        sys.exit(0)

    print("\n" + "=" * 80)
    print(f"TARGET ITEMS TO {action_word} AIOPS ({len(targets)} total):")
    print("=" * 80)
    for item in targets:
        type_label = "Service" if item["type"] == "service" else "Orchestration"
        print(f"  - ({type_label}) {item['name']} (ID: {item['id']})")
    print("=" * 80)

    if not args.force:
        confirm = input(
            f"\n⚠️ Are you sure you want to {action_word} AIOps for these items? Type 'YES' to proceed: "
        ).strip()
        if confirm != "YES":
            logger.info("Update cancelled by user.")
            sys.exit(0)

    logger.info(f"\nStarting AIOps {action_word.lower()} process...")
    start_time = time.time()

    items_to_update = [(t["id"], t["type"], t["name"]) for t in targets]
    results = reporter.update_items_batch(items_to_update, enabled)

    elapsed_time = time.time() - start_time

    output_filename = export_update_report_csv(results, prefix=args.output)

    successful = sum(1 for r in results if r["status"] == "Updated")
    failed = len(results) - successful

    print(f"\n{'='*50}")
    print(f"✓ Update complete in {elapsed_time:.2f}s")
    print(f"  Successful: {successful}")
    print(f"  Failed:     {failed}")
    if output_filename:
        print(f"✓ Output file: {output_filename}")
    print(f"{'='*50}\n")


def main() -> None:
    parser = build_parser()

    argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args(argv)

    logger.setLevel(logging.INFO if args.debug else logging.WARNING)

    if not (args.audit or args.enable or args.disable):
        parser.print_help()
        sys.exit(0)

    if (args.enable or args.disable) and not (args.file or args.interactive):
        parser.error("--enable/--disable requires either -f/--file or -i/--interactive")

    if args.audit and (args.file or args.interactive or args.force):
        parser.error("-f/--file, -i/--interactive, and --force can only be used with --enable/--disable")

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

        if args.enable or args.disable:
            run_update(reporter, args)
        else:
            run_audit(reporter, args)

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()