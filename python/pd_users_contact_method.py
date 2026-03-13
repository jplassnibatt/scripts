import requests
import time
import csv
import sys
import os
import logging
from typing import List, Dict, Tuple, Optional
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessingMetrics:
    """Thread-safe metrics tracking for API requests"""
    
    def __init__(self):
        self.start_time = time.time()
        self.total_requests = 0
        self.total_contact_methods = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.users_processed = 0
        self.lock = threading.Lock()

    def add_request(self, success: bool, contact_methods_count: int = 0):
        """Record a request result"""
        with self.lock:
            self.total_requests += 1
            if success:
                self.successful_requests += 1
                self.total_contact_methods += contact_methods_count
            else:
                self.failed_requests += 1

    def add_user_processed(self):
        """Increment users processed counter"""
        with self.lock:
            self.users_processed += 1

    def get_summary(self) -> Dict:
        """Get current metrics summary"""
        with self.lock:
            elapsed_time = time.time() - self.start_time
            return {
                "total_requests": self.total_requests,
                "successful_requests": self.successful_requests,
                "failed_requests": self.failed_requests,
                "total_contact_methods": self.total_contact_methods,
                "users_processed": self.users_processed,
                "elapsed_time": elapsed_time,
                "requests_per_second": self.total_requests / elapsed_time if elapsed_time > 0 else 0
            }

class RateLimiter:
    """Thread-safe rate limiter for API requests"""
    
    def __init__(self, max_requests_per_second: int = 10):
        self.max_requests_per_second = max_requests_per_second
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0
        self.lock = threading.Lock()
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits"""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            
            if time_since_last_request < self.min_interval:
                sleep_time = self.min_interval - time_since_last_request
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()

class PagerDutyAPI:
    """PagerDuty API client with rate limiting and error handling"""
    
    def __init__(self, api_token: str, max_concurrent_requests: int = 5, rate_limit: int = 8):
        """
        Initialize PagerDuty API client
        
        Args:
            api_token: PagerDuty API token
            max_concurrent_requests: Maximum concurrent API requests
            rate_limit: Maximum requests per second
        """
        self.base_url = "https://api.pagerduty.com"
        self.headers = {
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Authorization": f"Token token={api_token}",
            "Content-Type": "application/json"
        }
        self.max_concurrent_requests = max_concurrent_requests
        self.rate_limiter = RateLimiter(max_requests_per_second=rate_limit)
        self.metrics = ProcessingMetrics()
        self.max_retries = 3
        self.retry_delay = 2
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make an API request with rate limiting and retry logic
        
        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters
            
        Returns:
            Response object or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.wait_if_needed()
                response = self.session.request(method, url, **kwargs)
                
                # Handle rate limiting (429 status code)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Handle other HTTP errors
                if response.status_code == 401:
                    logger.error("Authentication failed. Please check your API token.")
                    raise requests.exceptions.HTTPError("Invalid API token")
                elif response.status_code == 403:
                    logger.error("Access forbidden. Check API token permissions.")
                    raise requests.exceptions.HTTPError("Forbidden")
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    return None
        
        return None

    def validate_token(self) -> bool:
        """
        Validate the API token
        
        Returns:
            True if token is valid, False otherwise
        """
        try:
            response = self._make_request("GET", f"{self.base_url}/users", params={"limit": 1})
            return response is not None and response.status_code == 200
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            return False

    def get_all_users(self) -> List[Dict]:
        """
        Get all users from PagerDuty with pagination
        
        Returns:
            List of user dictionaries
        """
        users = []
        offset = 0
        limit = 100

        logger.info("Fetching users from PagerDuty...")

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "total": True
            }

            response = self._make_request("GET", f"{self.base_url}/users", params=params)

            if not response:
                logger.error("Failed to fetch users")
                break

            if response.status_code == 200:
                data = response.json()
                batch_users = data.get("users", [])
                users.extend(batch_users)
                
                total = data.get('total', len(users))
                logger.info(f"Fetched {len(users)}/{total} users")
                
                self.metrics.add_request(True)

                if not data.get('more', False) or len(batch_users) < limit:
                    break

                offset += limit
            else:
                self.metrics.add_request(False)
                logger.error(f"Error fetching users: {response.status_code}")
                logger.error(response.text)
                break

        return users

    def get_user_contact_methods(self, user: Dict) -> Tuple[Dict, List[Dict], bool]:
        """
        Get contact methods for a specific user
        
        Args:
            user: User dictionary
            
        Returns:
            Tuple of (user, contact_methods, success)
        """
        try:
            response = self._make_request(
                "GET",
                f"{self.base_url}/users/{user['id']}/contact_methods"
            )

            if not response:
                self.metrics.add_request(False)
                return user, [], False

            if response.status_code == 200:
                contact_methods = response.json().get("contact_methods", [])
                
                # Format phone numbers with country code
                for method in contact_methods:
                    if method['type'] in ['phone_contact_method', 'sms_contact_method']:
                        country_code = method.get('country_code', '')
                        address = method.get('address', '')
                        if country_code and address:
                            method['address'] = f"+{country_code} {address}"

                self.metrics.add_request(True, len(contact_methods))
                self.metrics.add_user_processed()
                return user, contact_methods, True
            else:
                self.metrics.add_request(False)
                logger.warning(f"Error fetching contact methods for user {user.get('name', user['id'])}: {response.status_code}")
                return user, [], False
                
        except Exception as e:
            self.metrics.add_request(False)
            logger.error(f"Exception while fetching contact methods for user {user.get('name', user['id'])}: {str(e)}")
            return user, [], False

def process_user_batch(
    api: PagerDutyAPI,
    users: List[Dict],
    writer: csv.writer,
    lock: threading.Lock,
    batch_num: int,
    total_batches: int
) -> Dict:
    """
    Process a batch of users and write their contact methods to CSV
    
    Args:
        api: PagerDuty API client
        users: List of users to process
        writer: CSV writer object
        lock: Thread lock for CSV writing
        batch_num: Current batch number
        total_batches: Total number of batches
        
    Returns:
        Dictionary with batch metrics
    """
    batch_start_time = time.time()
    successful_requests = 0
    contact_methods_count = 0

    logger.info(f"Processing batch {batch_num}/{total_batches} ({len(users)} users)")

    with ThreadPoolExecutor(max_workers=api.max_concurrent_requests) as executor:
        future_to_user = {
            executor.submit(api.get_user_contact_methods, user): user
            for user in users
        }

        for future in as_completed(future_to_user):
            try:
                user, contact_methods, success = future.result()

                if success:
                    successful_requests += 1
                    contact_methods_count += len(contact_methods)

                # Write to CSV
                with lock:
                    if not contact_methods:
                        writer.writerow([
                            user['id'],
                            user['name'],
                            user['email'],
                            '',
                            '',
                            '',
                            ''
                        ])
                    else:
                        for method in contact_methods:
                            writer.writerow([
                                user['id'],
                                user['name'],
                                user['email'],
                                method['id'],
                                method['type'],
                                method.get('address', ''),
                                method.get('label', '')
                            ])
            except Exception as e:
                logger.error(f"Error processing future result: {e}")
                continue

    batch_time = time.time() - batch_start_time
    
    metrics = {
        "batch_size": len(users),
        "successful_requests": successful_requests,
        "contact_methods": contact_methods_count,
        "batch_time": batch_time,
        "requests_per_second": len(users) / batch_time if batch_time > 0 else 0
    }
    
    logger.info(f"Batch {batch_num} completed in {batch_time:.2f}s - "
                f"{successful_requests}/{len(users)} successful, "
                f"{contact_methods_count} contact methods found")
    
    return metrics

def create_contact_methods_csv(
    api_token: str,
    output_file: Optional[str] = None,
    batch_size: int = 20,
    max_concurrent_requests: int = 5,
    rate_limit: int = 8
):
    """
    Create a CSV file with all PagerDuty user contact methods
    
    Args:
        api_token: PagerDuty API token
        output_file: Output CSV filename (optional)
        batch_size: Number of users to process per batch
        max_concurrent_requests: Maximum concurrent API requests
        rate_limit: Maximum requests per second
    """
    print("\n" + "="*70)
    print("PAGERDUTY CONTACT METHODS EXPORTER".center(70))
    print("="*70 + "\n")
    
    # Initialize API client
    pd_api = PagerDutyAPI(
        api_token,
        max_concurrent_requests=max_concurrent_requests,
        rate_limit=rate_limit
    )

    # Validate API token
    logger.info("Validating API token...")
    if not pd_api.validate_token():
        logger.error("Invalid API token. Please check your API_TOKEN environment variable.")
        sys.exit(1)

    # Generate filename with timestamp
    current_datetime = datetime.now().strftime('%Y-%m-%d-%H%M%S')

    if output_file is None:
        output_file = f"pagerduty_contacts_{current_datetime}.csv"
    else:
        # Add timestamp to custom filename before the extension
        if '.' in output_file:
            name, ext = output_file.rsplit('.', 1)
            output_file = f"{name}_{current_datetime}.{ext}"
        else:
            output_file = f"{output_file}_{current_datetime}.csv"

    headers = [
        "User ID",
        "User Name",
        "User Email",
        "Contact Method ID",
        "Contact Type",
        "Contact Address",
        "Contact Label"
    ]

    csv_lock = threading.Lock()

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            # Fetch all users
            users = pd_api.get_all_users()
            total_users = len(users)

            if total_users == 0:
                logger.warning("No users found")
                return

            logger.info(f"Found {total_users} users")
            logger.info(f"Processing with batch size: {batch_size}")
            logger.info(f"Max concurrent requests per batch: {max_concurrent_requests}")
            logger.info(f"Rate limit: {rate_limit} requests/second\n")

            # Process users in batches
            total_batches = (total_users + batch_size - 1) // batch_size
            batch_metrics_list = []

            for i in range(0, len(users), batch_size):
                batch = users[i:i + batch_size]
                batch_num = i // batch_size + 1

                batch_metrics = process_user_batch(
                    pd_api,
                    batch,
                    writer,
                    csv_lock,
                    batch_num,
                    total_batches
                )
                batch_metrics_list.append(batch_metrics)

        # Print final summary
        final_metrics = pd_api.metrics.get_summary()
        
        print("\n" + "="*70)
        print("PROCESSING SUMMARY".center(70))
        print("="*70)
        print(f"\nTotal users processed: {final_metrics['users_processed']}/{total_users}")
        print(f"Total API requests: {final_metrics['total_requests']}")
        print(f"Successful requests: {final_metrics['successful_requests']}")
        print(f"Failed requests: {final_metrics['failed_requests']}")
        print(f"Total contact methods found: {final_metrics['total_contact_methods']}")
        print(f"Total processing time: {final_metrics['elapsed_time']:.2f} seconds")
        print(f"Average requests per second: {final_metrics['requests_per_second']:.2f}")
        print(f"\n✓ CSV file '{output_file}' created successfully!")
        print("="*70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user")
        logger.info(f"Partial results saved to {output_file}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error creating CSV file: {e}")
        sys.exit(1)

def main():
    """Main execution function"""
    # Get API token from environment variable
    api_token = os.getenv("API_TOKEN", "").strip()
    
    # Validate API token is set
    if not api_token or api_token == "YOUR_PAGERDUTY_API_TOKEN":
        logger.error("ERROR: API_TOKEN environment variable is not set!")
        logger.error("Please set your PagerDuty API token:")
        logger.error("  export API_TOKEN='your-api-token-here'")
        sys.exit(1)

    # Configuration
    OUTPUT_FILE = os.getenv("OUTPUT_FILE", None)  # Optional custom output filename
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))
    MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "5"))
    RATE_LIMIT = int(os.getenv("RATE_LIMIT", "8"))

    try:
        create_contact_methods_csv(
            api_token=api_token,
            output_file=OUTPUT_FILE,
            batch_size=BATCH_SIZE,
            max_concurrent_requests=MAX_CONCURRENT,
            rate_limit=RATE_LIMIT
        )
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()