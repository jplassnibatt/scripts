#!/usr/bin/env python3
import argparse
import csv
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


class PagerDutyAPI:
    """PagerDuty REST API v2 Client with thread-safe rate limiting."""

    def __init__(self, api_token: str, max_rate_per_second: int = 5):
        if not api_token:
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.min_interval = 1.0 / max_rate_per_second
        self.last_request = 0.0
        self.lock = threading.Lock()

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-WorkflowExporter/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces thread-safe client-side rate limiting ($Rate = 5\\text{ req/s}$)."""
        with self.lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request = time.time()

    def _request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes a rate-limited HTTP GET request with retry backoff."""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
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

    def get_all_incident_workflows(self) -> List[Dict[str, Any]]:
        """Fetch all incident workflows using offset pagination."""
        workflows = []
        offset = 0
        limit = 100

        logger.info("Fetching all incident workflows...")
        while True:
            params = {"limit": limit, "offset": offset}
            response = self._request(f"{self.base_url}/incident_workflows", params=params)
            if not response:
                break

            data = response.json()
            workflows.extend(data.get("incident_workflows", []))

            logger.info(f"Retrieved {len(workflows)} workflows...")

            if not data.get("more", False):
                break
            offset += limit

        return workflows

    def get_workflow_details(self, workflow_id: str) -> Dict[str, Any]:
        """Fetch detailed information for a specific workflow."""
        response = self._request(f"{self.base_url}/incident_workflows/{workflow_id}")
        if response:
            return response.json().get("incident_workflow", {})
        return {}


def process_single_workflow(
    api: PagerDutyAPI, workflow: Dict[str, Any], index: int, total: int
) -> List[Dict[str, Any]]:
    """Process a single workflow and extract its steps."""
    workflow_id = workflow.get("id")
    workflow_name = workflow.get("name")
    results = []

    if index % 10 == 0 or index == total:
        logger.info(f"Processing workflow {index}/{total}: {workflow_name} ({workflow_id})")

    try:
        workflow_details = api.get_workflow_details(workflow_id)
        steps = workflow_details.get("steps", [])
        is_enabled = workflow_details.get("is_enabled", False)

        if steps:
            for step in steps:
                results.append(
                    {
                        "workflow_id": workflow_id,
                        "workflow_name": workflow_name,
                        "is_enabled": is_enabled,
                        "step_name": step.get("name", ""),
                    }
                )
        else:
            results.append(
                {
                    "workflow_id": workflow_id,
                    "workflow_name": workflow_name,
                    "is_enabled": is_enabled,
                    "step_name": "No steps",
                }
            )
    except Exception as e:
        logger.error(f"Error processing workflow {workflow_id}: {e}")
        results.append(
            {
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "is_enabled": "Error",
                "step_name": f"Error: {str(e)}",
            }
        )

    return results


def extract_workflow_steps_parallel(
    api: PagerDutyAPI, workflows: List[Dict[str, Any]], max_workers: int
) -> List[Dict[str, Any]]:
    """Extract workflow and step information using parallel processing."""
    all_results = []
    total = len(workflows)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_workflow = {
            executor.submit(
                process_single_workflow, api, workflow, i + 1, total
            ): workflow
            for i, workflow in enumerate(workflows)
        }

        for future in as_completed(future_to_workflow):
            try:
                results = future.result()
                all_results.extend(results)
            except Exception as e:
                workflow = future_to_workflow[future]
                logger.error(f"Exception for workflow {workflow.get('id')}: {e}")

    return all_results


def export_to_csv(
    data: List[Dict[str, Any]], 
    prefix: Optional[str] = None, 
    default_prefix: str = "pagerduty_incident_workflows_steps"
) -> str:
    """Writes data to a safely versioned, dynamically named timestamped CSV file."""
    resolved_prefix = prefix or os.environ.get("OUTPUT_FILE") or default_prefix

    if resolved_prefix.endswith(".csv"):
        resolved_prefix = resolved_prefix[:-4]

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{resolved_prefix}_{timestamp}.csv"

    fieldnames = ["Incident Workflow ID", "Incident Workflow Name", "Is Enabled", "Step Name"]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        if data:
            for row in data:
                mapped_row = {
                    "Incident Workflow ID": row.get("workflow_id"),
                    "Incident Workflow Name": row.get("workflow_name"),
                    "Is Enabled": row.get("is_enabled"),
                    "Step Name": row.get("step_name"),
                }
                writer.writerow(mapped_row)

    logger.info(f"✓ CSV file created: {filename}")
    logger.info(f"✓ Total rows written: {len(data)}")
    return filename


def validate_worker_limit(value: str) -> int:
    """Argparse type validator ensuring workers remain within safe bounds."""
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid integer.")
    
    if ivalue < 1 or ivalue > 10:
        raise argparse.ArgumentTypeError(
            f"Worker count {ivalue} is invalid. Must be between 1 and 10 to protect API integrity."
        )
    return ivalue


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Builds CLI options for the extraction tool."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Incident Workflows Steps v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-w", "--workers",
        type=validate_worker_limit,
        default=5,
        help="Number of parallel worker threads (1-10 max)",
    )

    parser.add_argument(
        "-o", "--output", default="pagerduty_incident_workflows_steps", help="Custom CSV filename prefix"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed [INFO] level log messages",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logger.setLevel(logging.INFO if args.debug else logging.WARNING)

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get(
        "API_TOKEN"
    )
    if not api_token or api_token.strip() == "YOUR_API_TOKEN_HERE":
        logger.error(
            "ERROR: Missing API token. Export PAGERDUTY_API_TOKEN environment variable."
        )
        sys.exit(1)

    try:
        api = PagerDutyAPI(api_token)
        if not api.validate_token():
            sys.exit(1)

        start_time = time.time()

        workflows = api.get_all_incident_workflows()
        
        if not workflows:
            logger.info("No incident workflows found in the account.")
            sys.exit(0)

        logger.info(f"Extracting steps for {len(workflows)} workflows using {args.workers} workers...")
        results = extract_workflow_steps_parallel(api, workflows, args.workers)

        filename = export_to_csv(results, prefix=args.output)

        elapsed_time = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"✓ Processed {len(workflows)} workflows in {elapsed_time:.2f}s")
        print(f"✓ Output file: {filename}")
        print(f"{'='*50}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()