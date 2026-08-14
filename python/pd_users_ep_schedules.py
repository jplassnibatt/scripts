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
from typing import Dict, List, Optional, Tuple
import requests

__version__ = "1.2.0"

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ROLE_MAPPING = {
    "admin": "Global Admin",
    "limited_user": "Responder",
    "owner": "Account Owner",
    "read_only_user": "Stakeholder",
    "read_only_limited_user": "Limited Stakeholder",
    "user": "Full User",
    "observer": "Observer",
    "restricted_access": "Restricted Access",
}


class ThreadSafeRateLimiter:
    """Thread-safe rate limiter to enforce client-side request throttling."""

    def __init__(self, max_requests_per_second: int = 8):
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0.0
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Wait if necessary to respect rate limits."""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time

            if time_since_last_request < self.min_interval:
                time.sleep(self.min_interval - time_since_last_request)

            self.last_request_time = time.time()


class PagerDutyAPI:
    """PagerDuty REST API v2 client with thread-safe rate limiting and error handling."""

    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.rate_limiter = ThreadSafeRateLimiter(max_requests_per_second=rate_limit)
        self.max_retries = 3
        self.timeout = 30

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-UserAssignmentAnalyzer/{__version__}",
            }
        )

    def _make_request(
        self, method: str, url: str, **kwargs
    ) -> Optional[requests.Response]:
        """Make an API request with exponential backoff and rate-limit handling."""
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout

        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.acquire()
                response = self.session.request(method, url, **kwargs)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited ($Rate = {self.rate_limiter.min_interval:.3f}\\text{{s/req}}$). Waiting {retry_after}s..."
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN permissions."
                    )
                    sys.exit(1)

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Timeout encountered (attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error("Request timed out after maximum retries.")
                    return None

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                    )
                    time.sleep(2**attempt)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    return None

        return None

    def validate_token(self) -> bool:
        """Validate API token credentials against the `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._make_request("GET", f"{self.base_url}/users", params={"limit": 1})
        if response is not None and response.status_code == 200:
            logger.info("✓ API token validated successfully")
            return True
        return False

    def get_all_users(self) -> List[Dict]:
        """Fetch all users from PagerDuty using offset pagination."""
        users = []
        offset = 0
        limit = 100

        logger.info("Fetching users from PagerDuty...")

        while True:
            params = {"offset": offset, "limit": limit, "total": True}
            response = self._make_request("GET", f"{self.base_url}/users", params=params)

            if not response:
                logger.error("Failed to fetch users.")
                break

            if response.status_code == 200:
                data = response.json()
                batch_users = data.get("users", [])
                users.extend(batch_users)

                total = data.get("total", len(users))
                logger.info(f"Fetched {len(users)}/{total} users")

                if not data.get("more", False) or len(batch_users) < limit:
                    break

                offset += limit
            else:
                logger.error(f"Error fetching users: {response.status_code}")
                break

        return users

    def get_user_escalation_policies(self, user_id: str) -> List[Dict]:
        """Get escalation policies associated with a specific user."""
        response = self._make_request(
            "GET", f"{self.base_url}/users/{user_id}/escalation_policies"
        )
        if response:
            return response.json().get("escalation_policies", [])
        return []

    def get_user_schedules(self, user_id: str) -> List[Dict]:
        """Get schedules associated with a specific user."""
        response = self._make_request(
            "GET", f"{self.base_url}/users/{user_id}/schedules"
        )
        if response:
            return response.json().get("schedules", [])
        return []

    def get_user_assignments(self, user: Dict) -> Dict:
        """Get escalation policies and schedules for a user."""
        user_id = user.get("id", "")
        role_raw = user.get("role", "unknown")
        role_name = ROLE_MAPPING.get(role_raw, role_raw.replace("_", " ").title())

        policies = self.get_user_escalation_policies(user_id)
        schedules = self.get_user_schedules(user_id)

        return {
            "id": user_id,
            "name": user.get("name", "Unknown"),
            "email": user.get("email", "N/A"),
            "role": role_name,
            "policies": [{"id": p["id"], "name": p["name"]} for p in policies],
            "schedules": [{"id": s["id"], "name": s["name"]} for s in schedules],
        }


def process_users_assignments(
    api: PagerDutyAPI, users: List[Dict], max_workers: int = 5
) -> List[Dict]:
    """Process user assignments concurrently using ThreadPoolExecutor."""
    results = []
    total_users = len(users)

    logger.info(f"Analyzing assignments for {total_users} users with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_user = {
            executor.submit(api.get_user_assignments, user): user for user in users
        }

        completed = 0
        for future in as_completed(future_to_user):
            try:
                res = future.result()
                results.append(res)
                completed += 1

                if completed % 10 == 0 or completed == total_users:
                    logger.info(f"Progress: {completed}/{total_users} users analyzed")

            except Exception as e:
                user = future_to_user[future]
                logger.error(
                    f"Error analyzing user {user.get('name', user.get('id'))}: {e}"
                )
                results.append(
                    {
                        "id": user.get("id", ""),
                        "name": user.get("name", "Unknown"),
                        "email": user.get("email", "N/A"),
                        "role": user.get("role", "N/A"),
                        "policies": [],
                        "schedules": [],
                    }
                )
                completed += 1

    return results


def export_to_csv(
    results: List[Dict], 
    prefix: Optional[str] = None, 
    default_prefix: str = "pagerduty_users_ep_schedules"
) -> Optional[str]:
    """Export user assignment analysis to a dynamic, safely versioned timestamped CSV file."""
    if not results:
        logger.info("No data available to export.")
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
        "User ID",
        "User Name",
        "User Email",
        "User Role",
        "Has EPs",
        "Escalation Policies",
        "Has Schedules",
        "Schedules",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for user in results:
            writer.writerow(
                {
                    "User ID": user.get("id", "N/A"),
                    "User Name": user.get("name", "N/A"),
                    "User Email": user.get("email", "N/A"),
                    "User Role": user.get("role", "N/A"),
                    "Has EPs": "Yes" if user.get("policies") else "No",
                    "Escalation Policies": "; ".join(
                        [p["name"] for p in user.get("policies", [])]
                    ),
                    "Has Schedules": "Yes" if user.get("schedules") else "No",
                    "Schedules": "; ".join(
                        [s["name"] for s in user.get("schedules", [])]
                    ),
                }
            )

    logger.info(f"✓ CSV report saved to '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Custom help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Configure command line arguments."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Users Not in Schedules Nor in EP v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pagerduty_users_ep_schedules",
        help="Output CSV filename prefix",
    )
    parser.add_argument(
        "-w",
        "--max-workers",
        type=int,
        default=5,
        help="Maximum concurrent workers (1-20)",
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

    api_token = os.environ.get("PAGERDUTY_API_TOKEN") or os.environ.get("API_TOKEN")
    if not api_token:
        logger.error(
            "ERROR: Missing API token. Export PAGERDUTY_API_TOKEN environment variable."
        )
        sys.exit(1)

    if args.max_workers < 1 or args.max_workers > 20:
        logger.error("Max workers must be between 1 and 20.")
        sys.exit(1)
    if args.rate_limit < 1 or args.rate_limit > 100:
        logger.error("Rate limit must be between 1 and 100.")
        sys.exit(1)

    try:
        api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        if not api.validate_token():
            sys.exit(1)

        start_time = time.time()
        users = api.get_all_users()
        if not users:
            logger.warning("No users found.")
            sys.exit(0)

        results = process_users_assignments(
            api, users, max_workers=args.max_workers
        )
        
        output_filename = export_to_csv(results, prefix=args.output)

        elapsed = time.time() - start_time
        assigned_users = sum(1 for u in results if u["policies"] or u["schedules"])
        unassigned_users = len(results) - assigned_users

        print("\n" + "=" * 70)
        print("ANALYSIS SUMMARY".center(70))
        print("=" * 70)
        print(f"Total Users Analyzed:    {len(results)}")
        print(f"Users with Assignments:  {assigned_users}")
        print(f"Unassigned Users:        {unassigned_users}")
        print(f"Execution Time:          {elapsed:.2f}s")
        print(f"Output File:             {output_filename or 'N/A'}")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()