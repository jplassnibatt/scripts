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
from typing import Dict, List, Optional
import requests

__version__ = "1.2.0"

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
        self.successful_requests = 0
        self.failed_requests = 0
        self.users_processed = 0
        self.lock = threading.Lock()

    def add_request(self, success: bool) -> None:
        """Record a request result."""
        with self.lock:
            self.total_requests += 1
            if success:
                self.successful_requests += 1
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
                "users_processed": self.users_processed,
                "elapsed_time": elapsed_time,
                "requests_per_second": (
                    self.total_requests / elapsed_time if elapsed_time > 0 else 0
                ),
            }


class ThreadSafeRateLimiter:
    """Thread-safe rate limiter to enforce client-side request throttling."""

    def __init__(self, calls_per_second: int = 8):
        self.minimum_interval = 1.0 / calls_per_second
        self.last_call_time = 0.0
        self.lock = threading.Lock()

    def acquire(self) -> None:
        """Wait if necessary to respect rate limits."""
        with self.lock:
            now = time.time()
            time_since_last_call = now - self.last_call_time
            if time_since_last_call < self.minimum_interval:
                time.sleep(self.minimum_interval - time_since_last_call)
            self.last_call_time = time.time()


class PagerDutyAPI:
    """PagerDuty REST API v2 client with rate limiting, retries, and metric tracking."""

    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")

        self.base_url = "https://api.pagerduty.com"
        self.rate_limiter = ThreadSafeRateLimiter(calls_per_second=rate_limit)
        self.metrics = ProcessingMetrics()
        self.max_retries = 3
        self.timeout = 30

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Token token={api_token.strip()}",
                "Accept": "application/vnd.pagerduty+json;version=2",
                "Content-Type": "application/json",
                "User-Agent": f"PagerDutyDevBuddy-UserLicensesExporter/{__version__}",
            }
        )

    def _make_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Make an HTTP GET request with retry backoff and rate-limit handling."""
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.timeout

        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.acquire()
                response = self.session.get(url, **kwargs)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited ($Rate = {1.0 / self.rate_limiter.minimum_interval:.1f}\\text{{ req/s}}$). Waiting {retry_after}s..."
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code in (401, 403):
                    logger.error(
                        f"Authentication failed (HTTP {response.status_code}). Check PAGERDUTY_API_TOKEN permissions."
                    )
                    sys.exit(1)
                elif response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return response

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
        """Validate API token against `/users` endpoint."""
        logger.info("Validating API token...")
        response = self._make_request(f"{self.base_url}/users", params={"limit": 1})
        if response is not None and response.status_code == 200:
            logger.info("✓ API token is valid")
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
            response = self._make_request(f"{self.base_url}/users", params=params)

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

    def get_user_license(self, user: Dict) -> Dict:
        """Fetch license details for a specific user ID."""
        user_id = user.get("id", "")
        user_name = user.get("name", "Unknown")
        user_email = user.get("email", "N/A")

        try:
            url = f"{self.base_url}/users/{user_id}/license"
            response = self._make_request(url)

            if not response:
                self.metrics.add_request(False)
                return {
                    "user_id": user_id,
                    "name": user_name,
                    "email": user_email,
                    "license_name": "ERROR_FETCHING_LICENSE",
                    "license_description": "Failed to fetch license information",
                    "status": "error",
                }

            if response.status_code == 200:
                license_data = response.json()
                license_info = license_data.get("license", {})

                self.metrics.add_request(True)
                self.metrics.add_user_processed()

                return {
                    "user_id": user_id,
                    "name": user_name,
                    "email": user_email,
                    "license_name": license_info.get("name", "N/A"),
                    "license_description": license_info.get("description", "N/A"),
                    "status": "success",
                }
            else:
                self.metrics.add_request(False)
                return {
                    "user_id": user_id,
                    "name": user_name,
                    "email": user_email,
                    "license_name": "ERROR_FETCHING_LICENSE",
                    "license_description": f"HTTP {response.status_code}",
                    "status": "error",
                }

        except Exception as e:
            self.metrics.add_request(False)
            return {
                "user_id": user_id,
                "name": user_name,
                "email": user_email,
                "license_name": "ERROR_FETCHING_LICENSE",
                "license_description": str(e),
                "status": "error",
            }


def process_users_licenses(
    api: PagerDutyAPI, users: List[Dict], max_workers: int = 5
) -> List[Dict]:
    """Process user licenses in parallel using ThreadPoolExecutor."""
    processed_users = []
    total_users = len(users)

    logger.info(f"Processing {total_users} users with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_user = {
            executor.submit(api.get_user_license, user): user for user in users
        }

        completed = 0
        for future in as_completed(future_to_user):
            try:
                result = future.result()
                processed_users.append(result)
                completed += 1

                if completed % 10 == 0 or completed == total_users:
                    logger.info(f"Progress: {completed}/{total_users} users processed")

            except Exception as e:
                user = future_to_user[future]
                processed_users.append(
                    {
                        "user_id": user.get("id", ""),
                        "name": user.get("name", "Unknown"),
                        "email": user.get("email", "N/A"),
                        "license_name": "ERROR",
                        "license_description": str(e),
                        "status": "error",
                    }
                )
                completed += 1

    return processed_users


def export_to_csv(
    users_data: List[Dict], 
    prefix: Optional[str] = None, 
    default_prefix: str = "pagerduty_users_licenses"
) -> Optional[str]:
    """Export user license records to a safely versioned timestamped CSV file."""
    if not users_data:
        logger.info("No user data available to export.")
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
        "Name",
        "Email",
        "License Name",
        "License Description",
        "Status",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in users_data:
            mapped_row = {
                "User ID": row.get("user_id"),
                "Name": row.get("name"),
                "Email": row.get("email"),
                "License Name": row.get("license_name"),
                "License Description": row.get("license_description"),
                "Status": row.get("status"),
            }
            writer.writerow(mapped_row)

    logger.info(f"✓ CSV file created successfully: '{filename}'")
    return filename


class WideHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    """Help formatter providing extended spacing for flag alignment."""

    def __init__(self, prog: str):
        super().__init__(prog, max_help_position=40, width=110)


def build_parser() -> argparse.ArgumentParser:
    """Build command line arguments."""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty License Allocation v{__version__}",
        formatter_class=WideHelpFormatter,
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s v{__version__}"
    )
    parser.add_argument(
        "-o", "--output", default="pagerduty_users_licenses", help="Output CSV filename prefix"
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
        logger.error("ERROR: Missing API token. Export PAGERDUTY_API_TOKEN environment variable.")
        sys.exit(1)

    if args.max_workers < 1 or args.max_workers > 20:
        logger.error("Max workers must be between 1 and 20.")
        sys.exit(1)
    if args.rate_limit < 1 or args.rate_limit > 100:
        logger.error("Rate limit must be between 1 and 100.")
        sys.exit(1)

    try:
        pd_api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        if not pd_api.validate_token():
            sys.exit(1)

        users = pd_api.get_all_users()
        if not users:
            logger.warning("No users found.")
            sys.exit(0)

        processed_users = process_users_licenses(
            pd_api, users, max_workers=args.max_workers
        )
        
        # Utilize safely isolated output writing
        output_filename = export_to_csv(processed_users, prefix=args.output)

        final_metrics = pd_api.metrics.get_summary()
        successful_users = sum(1 for u in processed_users if u["status"] == "success")
        failed_users = sum(1 for u in processed_users if u["status"] == "error")

        print("\n" + "=" * 70)
        print("PROCESSING SUMMARY".center(70))
        print("=" * 70)
        print(f"Users Processed:    {len(processed_users)}/{len(users)}")
        print(f"  Successful:       {successful_users}")
        print(f"  Failed:           {failed_users}")
        print(f"API Requests:       {final_metrics['total_requests']}")
        print(f"  Requests/sec:     {final_metrics['requests_per_second']:.2f}")
        print(f"Elapsed Time:       {final_metrics['elapsed_time']:.2f}s")
        print(f"Output File:        {output_filename or 'N/A'}")
        print("=" * 70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user. Exiting safely.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()