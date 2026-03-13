import requests
import json
import os
import sys
import csv
from time import sleep, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Role mapping dictionary
ROLE_MAPPING = {
    'admin': 'Global Admin',
    'limited_user': 'Responder',
    'owner': 'Account Owner',
    'read_only_user': 'Stakeholder',
    'read_only_limited_user': 'Limited Stakeholder',
    'user': 'Full User',
    'observer': 'Observer',
    'restricted_access': 'Restricted Access'
}

class RateLimiter:
    """Simple rate limiter to respect API limits"""
    def __init__(self, max_requests_per_second: int = 10):
        self.max_requests_per_second = max_requests_per_second
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits"""
        current_time = time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_interval:
            sleep_time = self.min_interval - time_since_last_request
            sleep(sleep_time)
        
        self.last_request_time = time()

class PagerDutyAPI:
    def __init__(self, api_token: str, rate_limit: int = 10):
        """
        Initialize PagerDuty API client
        
        Args:
            api_token: PagerDuty API token
            rate_limit: Maximum requests per second (default: 10)
        """
        self.base_url = "https://api.pagerduty.com"
        self.headers = {
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Authorization": f"Token token={api_token}",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.rate_limiter = RateLimiter(max_requests_per_second=rate_limit)
        self.max_retries = 3
        self.retry_delay = 2  # seconds

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[requests.Response]:
        """
        Make an API request with rate limiting and retry logic
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object or None if all retries failed
        """
        for attempt in range(self.max_retries):
            try:
                self.rate_limiter.wait_if_needed()
                response = self.session.request(method, endpoint, **kwargs)
                
                # Handle rate limiting (429 status code)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 401:
                    logger.error("Authentication failed. Please check your API token.")
                    raise
                elif response.status_code == 403:
                    logger.error("Access forbidden. Check API token permissions.")
                    raise
                elif attempt < self.max_retries - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                    sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    raise
                    
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Request error (attempt {attempt + 1}/{self.max_retries}): {e}")
                    sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    return None
        
        return None

    def validate_token(self) -> bool:
        """
        Validate the API token by making a test request
        
        Returns:
            True if token is valid, False otherwise
        """
        try:
            endpoint = f"{self.base_url}/users"
            params = {"limit": 1}
            response = self._make_request("GET", endpoint, params=params)
            return response is not None and response.status_code == 200
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            return False

    def get_all_users(self) -> List[Dict]:
        """Get all users from PagerDuty with pagination"""
        endpoint = f"{self.base_url}/users"
        all_users = []
        offset = 0
        limit = 100

        while True:
            params = {
                "offset": offset,
                "limit": limit,
                "total": True,
                "include[]": ["contact_methods"]
            }

            response = self._make_request("GET", endpoint, params=params)
            if not response:
                logger.error("Failed to fetch users")
                break
                
            data = response.json()
            users = data.get('users', [])
            
            if not users:
                break

            all_users.extend(users)
            
            # Log progress
            total = data.get('total', len(all_users))
            logger.info(f"Fetched {len(all_users)}/{total} users")

            if not data.get('more', False):
                break

            offset += limit

        return all_users

    def get_user_escalation_policies(self, user_id: str) -> List[Dict]:
        """Get escalation policies for a specific user"""
        endpoint = f"{self.base_url}/users/{user_id}/escalation_policies"
        response = self._make_request("GET", endpoint)
        
        if response:
            return response.json().get('escalation_policies', [])
        return []

    def get_user_schedules(self, user_id: str) -> List[Dict]:
        """Get schedules for a specific user"""
        endpoint = f"{self.base_url}/users/{user_id}/schedules"
        response = self._make_request("GET", endpoint)
        
        if response:
            return response.json().get('schedules', [])
        return []

    def get_user_data(self, user_id: str) -> Tuple[List[Dict], List[Dict]]:
        """Get both escalation policies and schedules for a user"""
        # Sequential calls to respect rate limiting better
        policies = self.get_user_escalation_policies(user_id)
        schedules = self.get_user_schedules(user_id)
        return policies, schedules

def get_proper_role_name(role: str) -> str:
    """Convert API role to proper display name"""
    return ROLE_MAPPING.get(role, role.replace('_', ' ').title())

def analyze_all_users(api_token: str) -> List[Dict]:
    """
    Analyze all PagerDuty users and their assignments
    
    Args:
        api_token: PagerDuty API token
        
    Returns:
        List of user data dictionaries
    """
    pd_api = PagerDutyAPI(api_token, rate_limit=8)  # Conservative rate limit
    results = []

    try:
        logger.info("Validating API token...")
        if not pd_api.validate_token():
            logger.error("Invalid API token. Please check your API_TOKEN environment variable.")
            sys.exit(1)
        
        logger.info("Fetching all users...")
        users = pd_api.get_all_users()
        total_users = len(users)
        
        if total_users == 0:
            logger.warning("No users found")
            return results
            
        logger.info(f"Found {total_users} users")
        logger.info("Processing users...")

        # Process users sequentially to better control rate limiting
        for index, user in enumerate(users, 1):
            try:
                if index % 10 == 0 or index == total_users:
                    logger.info(f"Processing user {index}/{total_users}")

                policies, schedules = pd_api.get_user_data(user['id'])

                user_result = {
                    'id': user['id'],
                    'name': user['name'],
                    'email': user['email'],
                    'role': get_proper_role_name(user.get('role', 'unknown')),
                    'policies': [{
                        'id': p['id'],
                        'name': p['name']
                    } for p in policies],
                    'schedules': [{
                        'id': s['id'],
                        'name': s['name']
                    } for s in schedules]
                }
                results.append(user_result)
                
            except Exception as e:
                logger.error(f"Error processing user {user.get('name', 'unknown')}: {e}")
                continue

        logger.info("Analysis complete!")
        return results

    except KeyboardInterrupt:
        logger.warning("\nAnalysis interrupted by user")
        return results
    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}")
        return results

def print_analysis_results(results: List[Dict]):
    """Print the analysis results in a readable format"""
    print("\n" + "="*60)
    print("ANALYSIS RESULTS".center(60))
    print("="*60 + "\n")

    users_with_assignments = [u for u in results if u['policies'] or u['schedules']]
    users_without_assignments = [u for u in results if not u['policies'] and not u['schedules']]

    print(f"Total users analyzed: {len(results)}")
    print(f"Users with assignments: {len(users_with_assignments)}")
    print(f"Users without any assignments: {len(users_without_assignments)}")

    if users_with_assignments:
        print("\n" + "="*60)
        print("USERS WITH ASSIGNMENTS".center(60))
        print("="*60)
        
        for user in users_with_assignments:
            print(f"\n{'─'*60}")
            print(f"User: {user['name']}")
            print(f"Email: {user['email']}")
            print(f"Role: {user['role']}")

            if user['policies']:
                print("\nEscalation Policies:")
                for policy in user['policies']:
                    print(f"  • {policy['name']} (ID: {policy['id']})")

            if user['schedules']:
                print("\nSchedules:")
                for schedule in user['schedules']:
                    print(f"  • {schedule['name']} (ID: {schedule['id']})")

    if users_without_assignments:
        print("\n" + "="*60)
        print("USERS WITHOUT ANY ASSIGNMENTS".center(60))
        print("="*60)
        
        for user in users_without_assignments:
            print(f"\n{'─'*60}")
            print(f"User: {user['name']}")
            print(f"Email: {user['email']}")
            print(f"Role: {user['role']}")

    print("\n" + "="*60 + "\n")

def export_to_csv(results: List[Dict]) -> str:
    """
    Export results to CSV file with timestamp
    
    Args:
        results: List of user data dictionaries
        
    Returns:
        Filename of the exported CSV
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pagerduty_user_assignments_{timestamp}.csv"

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Name', 'Email', 'Role', 'Has EPs', 'Escalation Policies',
                         'Has Schedules', 'Schedules']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for user in results:
                writer.writerow({
                    'Name': user['name'],
                    'Email': user['email'],
                    'Role': user['role'],
                    'Has EPs': 'Yes' if user['policies'] else 'No',
                    'Escalation Policies': '; '.join([p['name'] for p in user['policies']]),
                    'Has Schedules': 'Yes' if user['schedules'] else 'No',
                    'Schedules': '; '.join([s['name'] for s in user['schedules']])
                })

        logger.info(f"Results exported to {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Failed to export CSV: {e}")
        return ""

def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("PAGERDUTY USER ASSIGNMENT ANALYZER".center(60))
    print("="*60 + "\n")
    
    # Get API token from environment variable
    API_TOKEN = os.getenv("API_TOKEN", "").strip()
    
    # Validate API token is set
    if not API_TOKEN or API_TOKEN == "YOUR_PAGERDUTY_API_TOKEN_HERE":
        logger.error("ERROR: API_TOKEN environment variable is not set!")
        logger.error("Please set your PagerDuty API token:")
        logger.error("  export API_TOKEN='your-api-token-here'")
        sys.exit(1)

    try:
        results = analyze_all_users(api_token=API_TOKEN)
        
        if not results:
            logger.warning("No results to display")
            return
            
        print_analysis_results(results)
        export_to_csv(results)
        
        logger.info("Process completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()