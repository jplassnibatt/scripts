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

__version__ = "1.1.0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ProcessingMetrics:
    """Thread-safe metrics tracking for API requests."""

    def __init__(self):
        self.start_time = time.time()
        self.total_requests = 0
        self.total_contact_methods = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.users_processed = 0
        self.lock = threading.Lock()

    def add_request(self, success: bool, contact_methods_count: int = 0) -> None:
        """Record a request result."""
        with self.lock:
            self.total_requests += 1
            if success:
                self.successful_requests += 1
                self.total_contact_methods += contact_methods_count
            else:
                self.failed_requests += 1

    def add_user_processed(self) -> None:
        """Increment users processed counter."""
        with self.lock:
            self.users_processed += 1

    def get_summary(self) -> Dict:
        """Get current metrics summary."""
        with self.lock:
            elapsed_time = time.time() - self.start_time
            return {
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "total_contact_methods": self.total_contact_methods,
                "users_processed": self.users_processed,
                "elapsed_time": elapsed_time,
                "requests_per_second": (
                    self.total_requests / elapsed_time if elapsed_time > 0 else 0
                ),
            }


class ThreadSafeRateLimiter:
    """Thread-safe rate limiter for client-side request throttling."""

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
        self.metrics = ProcessingMetrics()
        self.max_retries = 3
        self.timeout = 30

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Authorization": f"Token token={api_token.strip()}",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-ContactMethodsExporter/{__version__}",
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
                self.metrics.add_request(True)

                if not data.get("more", False) or len(batch_users) < limit:
                    break

                offset += limit
            else:
                self.metrics.add_request(False)
                logger.error(f"Error fetching users: {response.status_code}")
                break

        return users

    def get_user_contact_methods(self, user: Dict) -> Tuple[Dict, List[Dict], bool]:
        """Fetch contact methods for a specific user ID."""
        user_id = user.get("id", "")
        user_name = user.get("name", "Unknown")

        try:
            response = self._make_request(
                "GET", f"{self.base_url}/users/{user_id}/contact_methods"
            )

            if not response:
                self.metrics.add_request(False)
                return user, [], False

            if response.status_code == 200:
                contact_methods = response.json().get("contact_methods", [])

                for method in contact_methods:
                    if method.get("type") in ("phone_contact_method", "sms_contact_method"):
                        country_code = method.get("country_code", "")
                        address = method.get("address", "")
                        if country_code and address:
                            method["address"] = f"+{country_code} {address}"

                self.metrics.add_request(True, len(contact_methods))
                self.metrics.add_user_processed()
                return user, contact_methods, True
            else:
                self.metrics.add_request(False)
                logger.warning(
                    f"Error fetching contact methods for user {user_name} ({user_id}): {response.status_code}"
                )
                return user, [], False

        except Exception as e:
            self.metrics.add_request(False)
            logger.error(f"Exception fetching contact methods for user {user_name} ({user_id}): {e}")
            return user, [], False


def process_user_contact_methods(
    api: PagerDutyAPI, users: List[Dict], max_workers: int = 5
) -> List[Tuple[Dict, List[Dict]]]:
    """Process user contact methods concurrently using ThreadPoolExecutor."""
    results = []
    total_users = len(users)

    logger.info(f"Processing {total_users} users with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_user = {
            executor.submit(api.get_user_contact_methods, user): user for user in users
        }

        completed = 0
        for future in as_completed(future_to_user):
            try:
                user, contact_methods, _ = future.result()
                results.append((user, contact_methods))
                completed += 1

                if completed % 10 == 0 or completed == total_users:
                    logger.info(f"Progress: {completed}/{total_users} users processed")

            except Exception as e:
                user = future_to_user[future]
                logger.error(f"Error processing user {user.get('name', user.get('id'))}: {e}")
                results.append((user, []))
                completed += 1

    return results


def export_to_csv(
    data: List[Tuple[Dict, List[Dict]]], 
    prefix: Optional[str] = None, 
    default_prefix: str = "pagerduty_contacts_per_user"
) -> Optional[str]:
    """Export aggregated contact methods to a dynamic, safely versioned timestamped CSV file."""
    if not data:
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
        "Contact Method ID",
        "Contact Type",
        "Contact Address",
        "Contact Label",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)

        for user, contact_methods in data:
            user_id = user.get("id", "N/A")
            user_name = user.get("name", "Unknown")
            user_email = user.get("email", "N/A")

            if not contact_methods:
                writer.writerow([user_id, user_name, user_email, "", "", "", ""])
            else:
                for method in contact_methods:
                    writer.writerow(
                        [
                            user_id,
                            user_name,
                            user_email,
                            method.get("id", ""),
                            method.get("type", ""),
                            method.get("address", ""),
                            method.get("label", ""),
                        ]
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
        description=f"CSE - PagerDuty Users Contact Method v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_contacts_per_user", help="Output CSV filename prefix"
    )
    parser.add_argument(
        "-w", "--max-workers", type=int, default=5, help="Maximum concurrent workers (1-20)"
    )
    parser.add_argument(
        "-r", "--rate-limit", type=int, default=8, help="Maximum API requests per second"
    )
    return parser


def main() -> None:
    parser = build_parser()

    args = parser.parse_args()

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

        users = api.get_all_users()
        if not users:
            logger.warning("No users found.")
            sys.exit(0)

        results = process_user_contact_methods(
            api, users, max_workers=args.max_workers
        )
        
        # Utilize safely isolated output writing
        output_filename = export_to_csv(results, prefix=args.output)

        final_metrics = api.metrics.get_summary()

        print("\n" + "=" * 70)
        print("PROCESSING SUMMARY".center(70))
        print("=" * 70)
        print(f"Users Processed:          {final_metrics['users_processed']}/{len(users)}")
        print(f"Total API Requests:       {final_metrics['total_requests']}")
        print(f"Successful Requests:      {final_metrics['successful_requests']}")
        print(f"Failed Requests:          {final_metrics['failed_requests']}")
        print(f"Total Contacts Found:     {final_metrics['total_contact_methods']}")
        print(f"Processing Time:          {final_metrics['elapsed_time']:.2f}s")
        print(f"Avg Requests/sec:         {final_metrics['requests_per_second']:.2f}")
        print(f"Output File:              {output_filename or 'N/A'}")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()