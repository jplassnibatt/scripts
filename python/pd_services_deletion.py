#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
import requests

__version__ = "1.3.0"

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PagerDutyServiceDeleter:
    """PagerDuty REST API v2 Client for managing and deleting services."""

    def __init__(self, api_token: str, rate_limit: int = 960):
        if not api_token or api_token.strip() == "":
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.min_interval = 60.0 / rate_limit  # $Delay = \frac{60}{960} = 0.0625\text{ seconds}$
        self.last_request = 0.0

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-ServiceDeleter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces client-side rate limiting ($Rate = 960\\text{ req/min}$)."""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()

    def _request(
        self, method: str, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes an HTTP request with exponential backoff and 429 Retry-After handling."""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.session.request(method, url, params=params, timeout=30)

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
        response = self._request("GET", f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def fetch_all_services(self) -> List[Dict[str, str]]:
        """Fetches all services using offset pagination checking the `more` boolean."""
        logger.info("Fetching all services from PagerDuty...")
        services = []
        offset = 0
        limit = 100

        while True:
            params = {"offset": offset, "limit": limit}
            response = self._request("GET", f"{self.base_url}/services", params=params)
            if not response:
                break

            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON response: {e}")
                break

            for s in data.get("services", []):
                services.append(
                    {
                        "id": s.get("id", ""),
                        "name": s.get("name", "Unknown Service"),
                        "description": s.get("description", ""),
                    }
                )

            if not data.get("more", False):
                break

            offset += limit

        logger.info(f"✓ Total services retrieved: {len(services)}")
        return services

    def delete_service(self, service_id: str, service_name: str) -> Dict[str, Any]:
        """Deletes a single service by ID."""
        response = self._request("DELETE", f"{self.base_url}/services/{service_id}")

        if response is not None and response.status_code == 204:
            logger.info(f"✓ Deleted: {service_name} (ID: {service_id})")
            return {"id": service_id, "name": service_name, "status": "Deleted"}
        elif response is not None and response.status_code == 404:
            logger.warning(f"✗ Not found: {service_name} (ID: {service_id})")
            return {"id": service_id, "name": service_name, "status": "Not Found"}
        else:
            status = response.status_code if response is not None else "No Response"
            logger.error(f"✗ Failed: {service_name} (ID: {service_id}) - Status: {status}")
            return {"id": service_id, "name": service_name, "status": f"Failed ({status})"}


def read_services_from_csv(filename: str) -> List[Dict[str, str]]:
    """Reads service IDs and names from a CSV input file."""
    services = []
    try:
        with open(filename, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Flexible header matching
                service_id = row.get("Service ID") or row.get("id") or row.get("ID")
                service_name = row.get("Service Name") or row.get("name") or row.get("Name") or ""
                if service_id:
                    services.append({"id": service_id.strip(), "name": service_name.strip()})
        logger.info(f"✓ Loaded {len(services)} services from '{filename}'")
        return services
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


def prompt_interactive_selection(all_services: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Displays an interactive list of services and parses user selections."""
    if not all_services:
        logger.info("No services available to select.")
        return []

    print("\n" + "=" * 80)
    print("AVAILABLE PAGERDUTY SERVICES")
    print("=" * 80)
    for idx, service in enumerate(all_services, 1):
        print(f"[{idx:3d}] {service['name']} (ID: {service['id']})")
    print("=" * 80)

    print("\nSelection options:")
    print("  - Single/Comma-separated numbers: e.g., 1, 3, 5")
    print("  - Range of numbers: e.g., 1-5")
    print("  - Combination: e.g., 1-3, 5, 8-10")

    user_input = input("\nEnter service numbers to delete (or 'cancel' to exit): ").strip()
    if user_input.lower() in ("cancel", "exit", "q", ""):
        logger.info("Interactive selection cancelled.")
        sys.exit(0)

    selected_indices = parse_selection_input(user_input, len(all_services))
    selected_services = [all_services[i] for i in selected_indices]

    return selected_services


def export_report_csv(
    results: List[Dict[str, Any]], 
    prefix: Optional[str] = None, 
    default_prefix: str = "pagerduty_service_deletion"
) -> Optional[str]:
    """Exports deletion execution results to a safely versioned timestamped CSV file."""
    if not results:
        logger.info("No deletion results to export.")
        return None

    # 1. Resolve fallback hierarchy: Explicit CLI arg -> Environment Var -> Default
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    # 2. Sanitize extension if user explicitly passed `.csv`
    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    # 3. Construct dynamic collision-proof timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"
    
    fieldnames = ["Service ID", "Service Name", "Status"]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            mapped_row = {
                "Service ID": row.get("id"),
                "Service Name": row.get("name"),
                "Status": row.get("status"),
            }
            writer.writerow(mapped_row)

    logger.info(f"✓ Deletion execution report saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Service Deletion v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-f", "--file", help="CSV input file containing services (columns: 'Service ID', 'Service Name')"
    )
    group.add_argument(
        "-i", "--interactive", action="store_true", help="Interactively fetch and select services from PagerDuty"
    )

    parser.add_argument(
        "-o", "--output", default="pagerduty_service_deletion", help="Custom CSV output filename prefix for the deletion report"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt and execute actual deletion immediately",
    )
    return parser


def main() -> None:
    parser = build_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get("API_TOKEN")
    if not api_token:
        logger.error("Missing API token. Export PAGERDUTY_API_TOKEN environment variable.")
        sys.exit(1)

    deleter = PagerDutyServiceDeleter(api_token)
    if not deleter.validate_token():
        sys.exit(1)

    if args.file:
        targets = read_services_from_csv(args.file)
    else:
        all_services = deleter.fetch_all_services()
        targets = prompt_interactive_selection(all_services)

    if not targets:
        logger.info("No services selected for deletion. Exiting.")
        sys.exit(0)

    print("\n" + "=" * 80)
    print(f"TARGET SERVICES FOR DELETION ({len(targets)} total):")
    print("=" * 80)
    for service in targets:
        print(f"  - {service['name']} (ID: {service['id']})")
    print("=" * 80)

    if not args.force:
        confirm = input("\n⚠️ Are you sure you want to PERMANENTLY delete these services? Type 'YES' to proceed: ").strip()
        if confirm != "YES":
            logger.info("Deletion cancelled by user.")
            sys.exit(0)

    logger.info("\nStarting service deletion process...")
    results = []
    start_time = time.time()

    for idx, service in enumerate(targets, 1):
        res = deleter.delete_service(service["id"], service["name"])
        results.append(res)

    elapsed_time = time.time() - start_time
    
    output_filename = export_report_csv(results, prefix=args.output)

    successful = sum(1 for r in results if r["status"] == "Deleted")
    failed = len(results) - successful

    print(f"\n{'='*50}")
    print(f"✓ Deletion complete in {elapsed_time:.2f}s")
    print(f"  Successful: {successful}")
    print(f"  Failed:     {failed}")
    if output_filename:
        print(f"✓ Output file: {output_filename}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()