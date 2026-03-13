import requests
import csv
import time
import sys
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from datetime import datetime
import argparse

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
        self.successful_requests = 0
        self.failed_requests = 0
        self.users_processed = 0
        self.lock = threading.Lock()

    def add_request(self, success: bool):
        """Record a request result"""
        with self.lock:
            self.total_requests += 1
            if success:
                self.successful_requests += 1
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
                "users_processed": self.users_processed,
                "elapsed_time": elapsed_time,
                "requests_per_second": self.total_requests / elapsed_time if elapsed_time > 0 else 0
            }

class RateLimiter:
    """Thread-safe rate limiter for API requests"""
    
    def __init__(self, calls_per_second: int = 10):
        self.minimum_interval = 1.0 / calls_per_second
        self.last_call_time = 0
        self.lock = threading.Lock()

    def acquire(self):
        """Wait if necessary to respect rate limits"""
        with self.lock:
            now = time.time()
            time_since_last_call = now - self.last_call_time
            if time_since_last_call < self.minimum_interval:
                time.sleep(self.minimum_interval - time_since_last_call)
            self.last_call_time = time.time()

class PagerDutyAPI:
    """PagerDuty API client with rate limiting and error handling"""
    
    def __init__(self, api_token: str, rate_limit: int = 8):
        """
        Initialize PagerDuty API client
        
        Args:
            api_token: PagerDuty API token
            rate_limit: Maximum requests per second
        """
        if not api_token or not api_token.strip():
            raise ValueError("API token cannot be empty")
            
        self.base_url = "https://api.pagerduty.com"
        self.headers = {
            'Authorization': f'Token token={api_token.strip()}',
            'Accept': 'application/vnd.pagerduty+json;version=2',
            'Content-Type': 'application/json'
        }
        self.rate_limiter = RateLimiter(calls_per_second=rate_limit)
        self.metrics = ProcessingMetrics()
        self.max_retries = 3
        self.retry_delay = 2
        self.timeout = 30
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        Make an API request with rate limiting and retry logic
        
        Args:
            url: Request URL
            **kwargs: Additional request parameters
            
        Returns:
            Response object or None if failed
        """
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
            
        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.acquire()
                response = self.session.get(url, **kwargs)

                # Handle rate limiting (429 status code)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                # Handle authentication errors
                if response.status_code == 401:
                    logger.error("Authentication failed. Please check your API token.")
                    raise requests.exceptions.HTTPError("Invalid API token")
                elif response.status_code == 403:
                    logger.error("Access forbidden. Check API token permissions.")
                    raise requests.exceptions.HTTPError("Forbidden")
                elif response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return response

                response.raise_for_status()
                return response

            except requests.exceptions.Timeout:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request timeout (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Request timed out after {self.max_retries} attempts")
                    return None

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
            logger.info("Validating API token...")
            response = self._make_request(f"{self.base_url}/users", params={"limit": 1})
            if response is not None and response.status_code == 200:
                logger.info("✓ API token is valid")
                return True
            return False
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

            response = self._make_request(f"{self.base_url}/users", params=params)

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
                if response.text:
                    logger.error(f"Response: {response.text[:200]}")
                break

        return users

    def get_user_license(self, user: Dict) -> Dict:
        """
        Get license information for a specific user
        
        Args:
            user: User dictionary
            
        Returns:
            Dictionary with user and license information
        """
        try:
            url = f"{self.base_url}/users/{user['id']}/license"
            response = self._make_request(url)

            if not response:
                self.metrics.add_request(False)
                logger.warning(f"Failed to fetch license for user: {user.get('name', user['id'])}")
                return {
                    'user_id': user['id'],
                    'name': user['name'],
                    'email': user['email'],
                    'license_name': 'ERROR_FETCHING_LICENSE',
                    'license_description': 'Failed to fetch license information',
                    'status': 'error'
                }

            if response.status_code == 200:
                license_data = response.json()
                license_info = license_data.get('license', {})
                
                self.metrics.add_request(True)
                self.metrics.add_user_processed()
                
                return {
                    'user_id': user['id'],
                    'name': user['name'],
                    'email': user['email'],
                    'license_name': license_info.get('name', 'N/A'),
                    'license_description': license_info.get('description', 'N/A'),
                    'status': 'success'
                }
            else:
                self.metrics.add_request(False)
                logger.warning(f"Error fetching license for user {user.get('name', user['id'])}: {response.status_code}")
                return {
                    'user_id': user['id'],
                    'name': user['name'],
                    'email': user['email'],
                    'license_name': 'ERROR_FETCHING_LICENSE',
                    'license_description': f'HTTP {response.status_code}',
                    'status': 'error'
                }

        except Exception as e:
            self.metrics.add_request(False)
            logger.error(f"Exception while fetching license for user {user.get('name', user['id'])}: {str(e)}")
            return {
                'user_id': user['id'],
                'name': user['name'],
                'email': user['email'],
                'license_name': 'ERROR_FETCHING_LICENSE',
                'license_description': str(e),
                'status': 'error'
            }

def process_users_licenses(
    api: PagerDutyAPI,
    users: List[Dict],
    max_workers: int = 5
) -> List[Dict]:
    """
    Process users and fetch their license information
    
    Args:
        api: PagerDuty API client
        users: List of users to process
        max_workers: Maximum concurrent workers
        
    Returns:
        List of user license dictionaries
    """
    processed_users = []
    total_users = len(users)
    
    logger.info(f"Processing {total_users} users with {max_workers} workers...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_user = {
            executor.submit(api.get_user_license, user): user
            for user in users
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
                logger.error(f"Error processing user {user.get('name', user['id'])}: {e}")
                processed_users.append({
                    'user_id': user['id'],
                    'name': user['name'],
                    'email': user['email'],
                    'license_name': 'ERROR',
                    'license_description': str(e),
                    'status': 'error'
                })
                completed += 1

    return processed_users

def export_to_csv(
    users_data: List[Dict],
    output_file: Optional[str] = None
) -> str:
    """
    Export user license data to CSV
    
    Args:
        users_data: List of user license dictionaries
        output_file: Optional output filename
        
    Returns:
        Name of the created CSV file
    """
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    if output_file is None:
        output_file = f'pagerduty_users_licenses_{timestamp}.csv'
    else:
        # Add timestamp to custom filename before the extension
        if '.' in output_file:
            name, ext = output_file.rsplit('.', 1)
            output_file = f"{name}_{timestamp}.{ext}"
        else:
            output_file = f"{output_file}_{timestamp}.csv"

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['user_id', 'name', 'email', 'license_name', 'license_description', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(users_data)

        logger.info(f"✓ CSV file '{output_file}' created successfully")
        return output_file
        
    except IOError as e:
        logger.error(f"Error writing to CSV file: {e}")
        raise

def create_users_licenses_report(
    api_token: str,
    output_file: Optional[str] = None,
    max_workers: int = 5,
    rate_limit: int = 8
):
    """
    Create a CSV report of all PagerDuty users and their licenses
    
    Args:
        api_token: PagerDuty API token
        output_file: Output CSV filename (optional)
        max_workers: Maximum concurrent workers
        rate_limit: Maximum requests per second
    """
    print("\n" + "="*70)
    print("PAGERDUTY USER LICENSES EXPORTER".center(70))
    print("="*70 + "\n")
    
    # Initialize API client
    try:
        pd_api = PagerDutyAPI(api_token, rate_limit=rate_limit)
    except ValueError as e:
        logger.error(f"Failed to initialize API client: {e}")
        sys.exit(1)

    # Validate API token
    if not pd_api.validate_token():
        logger.error("Invalid API token. Please check your API_TOKEN environment variable.")
        sys.exit(1)

    try:
        # Fetch all users
        users = pd_api.get_all_users()
        total_users = len(users)

        if total_users == 0:
            logger.warning("No users found")
            return

        logger.info(f"Found {total_users} users")
        logger.info(f"Configuration:")
        logger.info(f"  - Max concurrent workers: {max_workers}")
        logger.info(f"  - Rate limit: {rate_limit} requests/second\n")

        # Process users and fetch licenses
        processed_users = process_users_licenses(pd_api, users, max_workers)

        # Export to CSV
        output_filename = export_to_csv(processed_users, output_file)

        # Print final summary
        final_metrics = pd_api.metrics.get_summary()
        
        successful_users = sum(1 for u in processed_users if u['status'] == 'success')
        failed_users = sum(1 for u in processed_users if u['status'] == 'error')
        
        print("\n" + "="*70)
        print("PROCESSING SUMMARY".center(70))
        print("="*70)
        print(f"\nUsers:")
        print(f"  Total processed: {len(processed_users)}/{total_users}")
        print(f"  Successful: {successful_users}")
        print(f"  Failed: {failed_users}")
        print(f"\nAPI Requests:")
        print(f"  Total requests: {final_metrics['total_requests']}")
        print(f"  Successful: {final_metrics['successful_requests']}")
        print(f"  Failed: {final_metrics['failed_requests']}")
        print(f"\nPerformance:")
        print(f"  Total time: {final_metrics['elapsed_time']:.2f} seconds")
        print(f"  Avg requests/sec: {final_metrics['requests_per_second']:.2f}")
        print(f"\n✓ Report saved to '{output_filename}'")
        print("="*70 + "\n")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Export PagerDuty user licenses to CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables:
  API_TOKEN        PagerDuty API token (required)
  OUTPUT_FILE      Custom output filename (optional)
  MAX_WORKERS      Max concurrent workers (default: 5)
  RATE_LIMIT       Max requests per second (default: 8)

Examples:
  export API_TOKEN='your-token-here'
  python script.py
  python script.py --max-workers 10 --rate-limit 10
  python script.py --output licenses.csv
        """
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output CSV filename (overrides OUTPUT_FILE env var)'
    )
    parser.add_argument(
        '--max-workers', '-w',
        type=int,
        help='Maximum concurrent workers (default: 5)'
    )
    parser.add_argument(
        '--rate-limit', '-r',
        type=int,
        help='Maximum requests per second (default: 8)'
    )
    
    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_arguments()
    
    # Get API token from environment variable
    api_token = os.getenv("API_TOKEN", "").strip()
    
    # Validate API token is set
    if not api_token or api_token == "YOUR_API_TOKEN":
        logger.error("ERROR: API_TOKEN environment variable is not set!")
        logger.error("Please set your PagerDuty API token:")
        logger.error("  export API_TOKEN='your-api-token-here'")
        logger.error("\nFor help, run: python script.py --help")
        sys.exit(1)

    # Configuration - command line args override environment variables
    OUTPUT_FILE = args.output or os.getenv("OUTPUT_FILE", None)
    MAX_WORKERS = args.max_workers or int(os.getenv("MAX_WORKERS", "5"))
    RATE_LIMIT = args.rate_limit or int(os.getenv("RATE_LIMIT", "8"))
    
    # Validate configuration
    if MAX_WORKERS < 1 or MAX_WORKERS > 20:
        logger.error("Max workers must be between 1 and 20")
        sys.exit(1)
    if RATE_LIMIT < 1 or RATE_LIMIT > 100:
        logger.error("Rate limit must be between 1 and 100")
        sys.exit(1)

    try:
        create_users_licenses_report(
            api_token=api_token,
            output_file=OUTPUT_FILE,
            max_workers=MAX_WORKERS,
            rate_limit=RATE_LIMIT
        )
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()