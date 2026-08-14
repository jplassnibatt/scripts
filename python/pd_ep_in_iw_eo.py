#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Set

import requests

__version__ = "1.3.0"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class PagerDutyAPI:
    """PagerDuty REST API v2 Client with built-in rate limiting and pagination."""

    def __init__(self, api_token: str, max_rate_per_second: int = 8):
        if not api_token:
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.min_interval = 1.0 / max_rate_per_second
        self.last_request = 0.0
        self.rate_lock = threading.Lock()
        self.session = requests.Session()

        # Enforcing strict header compliance
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-OrchestrationAnalyzer/{__version__}",
            }
        )

    def _rate_limit(self) -> None:
        """Enforces thread-safe client-side rate limiting."""
        with self.rate_lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request = time.time()

    def request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> Optional[requests.Response]:
        """Makes an HTTP GET request with robust error handling and rate-limit backoff."""
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
                    logger.error(f"API Error fetching {url}: {str(e)}")
                    return None
        return None

    def validate_token(self) -> bool:
        """Validates API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self.request(f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def paginated_get(self, endpoint: str, data_key: str) -> List[dict]:
        """Generic function to fetch paginated data checking the 'more' boolean."""
        items = []
        url = f"{self.base_url}{endpoint}"
        offset = 0
        limit = 100

        while True:
            params = {"offset": offset, "limit": limit}
            response = self.request(url, params=params)

            if not response:
                break

            data = response.json()
            items.extend(data.get(data_key, []))

            if not data.get("more", False):
                break
            offset += limit

        return items


class OrchestrationAnalyzer:
    """Analyzes Event Orchestrations and Incident Workflows for Escalation Policy dependencies."""

    def __init__(self, api: PagerDutyAPI):
        self.api = api
        self.subdomain = None

    @staticmethod
    def extract_subdomain(html_url: str) -> Optional[str]:
        """Extracts subdomain from a PagerDuty HTML URL."""
        match = re.match(r"https://([^.]+)\.pagerduty\.com", html_url)
        return match.group(1) if match else None

    @staticmethod
    def find_ep_ids_in_json(data: dict, ep_ids: Set[str]) -> Set[str]:
        """Optimized search for specific Escalation Policy IDs within deeply nested JSON."""
        data_str = json.dumps(data)
        return {ep_id for ep_id in ep_ids if ep_id in data_str}

    @staticmethod
    def extract_rule_ids(data: dict, ep_id: str) -> List[str]:
        """Recursively extracts rule IDs referencing the specified Escalation Policy."""
        rule_ids = []

        def search_rules(obj, current_rule_id=None):
            if isinstance(obj, dict):
                if "id" in obj and "actions" in obj:
                    current_rule_id = obj["id"]
                    if (
                        ep_id in json.dumps(obj)
                        and current_rule_id not in rule_ids
                    ):
                        rule_ids.append(current_rule_id)
                for value in obj.values():
                    search_rules(value, current_rule_id)
            elif isinstance(obj, list):
                for item in obj:
                    search_rules(item, current_rule_id)

        search_rules(data)
        return rule_ids

    def get_escalation_policies(self) -> Dict[str, str]:
        logger.info("Fetching Escalation Policies...")
        policies = self.api.paginated_get(
            "/escalation_policies", "escalation_policies"
        )
        logger.info(f"✓ Fetched {len(policies)} Escalation Policies")
        return {p["id"]: p["name"] for p in policies}

    def get_services(self) -> List[Dict[str, str]]:
        logger.info("Fetching Services...")
        services_data = self.api.paginated_get("/services", "services")

        for service in services_data:
            if "html_url" in service:
                self.subdomain = self.extract_subdomain(service["html_url"])
                if self.subdomain:
                    logger.info(
                        f"✓ Auto-detected PagerDuty subdomain: {self.subdomain}"
                    )
                    break

        logger.info(f"✓ Fetched {len(services_data)} Services")
        return [{"id": s["id"], "name": s["name"]} for s in services_data]

    def process_workflows(self, ep_map: Dict[str, str], max_workers: int = 5) -> List[dict]:
        logger.info("Analyzing Incident Workflows...")
        workflows_data = self.api.paginated_get(
            "/incident_workflows", "incident_workflows"
        )
        ep_ids_set = set(ep_map.keys())
        matches = []

        def process_one(wf: Dict) -> List[dict]:
            response = self.api.request(
                f"{self.api.base_url}/incident_workflows/{wf['id']}"
            )
            if not response:
                return []

            details = response.json().get("incident_workflow", {})
            found_eps = self.find_ep_ids_in_json(details, ep_ids_set)

            results = []
            for ep_id in found_eps:
                url = (
                    f"https://{self.subdomain}.pagerduty.com/incident-workflows/workflows/{wf['id']}"
                    if self.subdomain
                    else ""
                )
                results.append(
                    {
                        "incident_workflow_id": wf["id"],
                        "incident_workflow_name": wf["name"],
                        "event_orchestration_name": "",
                        "service_name": "",
                        "escalation_policy_id": ep_id,
                        "escalation_policy_name": ep_map[ep_id],
                        "url": url,
                    }
                )
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_wf = {
                executor.submit(process_one, wf): wf for wf in workflows_data
            }
            for future in as_completed(future_to_wf):
                try:
                    matches.extend(future.result())
                except Exception as e:
                    wf = future_to_wf[future]
                    logger.error(f"Error processing workflow {wf.get('id')}: {e}")

        return matches

    def process_global_orchestrations(
        self, ep_map: Dict[str, str], max_workers: int = 5
    ) -> List[dict]:
        logger.info("Analyzing Global Event Orchestrations...")
        orchestrations = self.api.paginated_get(
            "/event_orchestrations", "orchestrations"
        )
        ep_ids_set = set(ep_map.keys())
        matches = []

        def process_one(orch: Dict) -> List[dict]:
            response = self.api.request(
                f"{self.api.base_url}/event_orchestrations/{orch['id']}/global"
            )
            if not response:
                return []

            details = response.json()
            found_eps = self.find_ep_ids_in_json(details, ep_ids_set)

            results = []
            for ep_id in found_eps:
                rule_ids = self.extract_rule_ids(details, ep_id)
                for rule_id in rule_ids or [None]:
                    url = (
                        f"https://{self.subdomain}.pagerduty.com/event-orchestration/{orch['id']}/global-rules/rule/{rule_id}"
                        if self.subdomain and rule_id
                        else ""
                    )
                    results.append(
                        {
                            "incident_workflow_id": "",
                            "incident_workflow_name": "",
                            "event_orchestration_name": orch.get(
                                "name", orch["id"]
                            ),
                            "service_name": "",
                            "escalation_policy_id": ep_id,
                            "escalation_policy_name": ep_map[ep_id],
                            "url": url,
                        }
                    )
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_orch = {
                executor.submit(process_one, orch): orch for orch in orchestrations
            }
            for future in as_completed(future_to_orch):
                try:
                    matches.extend(future.result())
                except Exception as e:
                    orch = future_to_orch[future]
                    logger.error(f"Error processing orchestration {orch.get('id')}: {e}")

        return matches

    def process_service_orchestrations(
        self, services: List[Dict[str, str]], ep_map: Dict[str, str], max_workers: int = 5
    ) -> List[dict]:
        logger.info("Analyzing Service Event Orchestrations...")
        ep_ids_set = set(ep_map.keys())
        matches = []

        def process_one(svc: Dict[str, str]) -> List[dict]:
            response = self.api.request(
                f"{self.api.base_url}/event_orchestrations/services/{svc['id']}"
            )
            if not response:
                return []

            details = response.json()
            found_eps = self.find_ep_ids_in_json(details, ep_ids_set)

            results = []
            for ep_id in found_eps:
                rule_ids = self.extract_rule_ids(details, ep_id)
                for rule_id in rule_ids or [None]:
                    url = (
                        f"https://{self.subdomain}.pagerduty.com/event-orchestration/service/{svc['id']}/rule/{rule_id}"
                        if self.subdomain and rule_id
                        else ""
                    )
                    results.append(
                        {
                            "incident_workflow_id": "",
                            "incident_workflow_name": "",
                            "event_orchestration_name": "",
                            "service_name": svc["name"],
                            "escalation_policy_id": ep_id,
                            "escalation_policy_name": ep_map[ep_id],
                            "url": url,
                        }
                    )
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_svc = {
                executor.submit(process_one, svc): svc for svc in services
            }
            for future in as_completed(future_to_svc):
                try:
                    matches.extend(future.result())
                except Exception as e:
                    svc = future_to_svc[future]
                    logger.error(f"Error processing service {svc.get('id')}: {e}")

        return matches


def export_to_csv(
    data: List[dict],
    prefix: Optional[str] = None,
    default_prefix: str = "pagerduty_ep_dependencies",
) -> Optional[str]:
    """Exports dependency matches to a safely versioned timestamped CSV file."""
    if not data:
        logger.info("No dependency matches found. Skipping CSV generation.")
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
        "Incident Workflow ID",
        "Incident Workflow Name",
        "Event Orchestration Name",
        "Service Name",
        "Escalation Policy ID",
        "Escalation Policy Name",
        "URL",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            mapped_row = {
                "Incident Workflow ID": row.get("incident_workflow_id"),
                "Incident Workflow Name": row.get("incident_workflow_name"),
                "Event Orchestration Name": row.get("event_orchestration_name"),
                "Service Name": row.get("service_name"),
                "Escalation Policy ID": row.get("escalation_policy_id"),
                "Escalation Policy Name": row.get("escalation_policy_name"),
                "URL": row.get("url"),
            }
            writer.writerow(mapped_row)

    logger.info(f"✓ CSV file generated: {filename}")
    logger.info(f"✓ Total matches mapped: {len(data)}")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Escalation Policy Dependency v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_ep_dependencies",
        help="Custom CSV filename prefix",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=5,
        help="Number of concurrent worker threads",
    )
    parser.add_argument(
        "-r",
        "--rate-limit",
        type=int,
        default=8,
        help="Maximum API requests per second",
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
            "ERROR: PAGERDUTY_API_TOKEN environment variable is missing or invalid."
        )
        logger.error(
            "Set it using: export PAGERDUTY_API_TOKEN='your-token-here'"
        )
        sys.exit(1)

    try:
        start_time = time.time()

        api = PagerDutyAPI(api_token, max_rate_per_second=args.rate_limit)
        if not api.validate_token():
            sys.exit(1)

        analyzer = OrchestrationAnalyzer(api)

        ep_map = analyzer.get_escalation_policies()
        if not ep_map:
            logger.info("No Escalation Policies found in the account. Exiting.")
            sys.exit(0)

        services = analyzer.get_services()

        # Aggregate matches
        matches = []
        matches.extend(analyzer.process_workflows(ep_map, max_workers=args.workers))
        matches.extend(
            analyzer.process_global_orchestrations(ep_map, max_workers=args.workers)
        )
        matches.extend(
            analyzer.process_service_orchestrations(
                services, ep_map, max_workers=args.workers
            )
        )

        # Utilize safely isolated output writing
        output_filename = export_to_csv(matches, prefix=args.output)

        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"✓ Escalation Policies analyzed: {len(ep_map)}")
        print(f"✓ Dependency matches found:     {len(matches)}")
        print(f"✓ Execution time:               {elapsed:.2f}s")
        print(f"✓ Output file:                  {output_filename or 'N/A'}")
        print(f"{'='*50}\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()