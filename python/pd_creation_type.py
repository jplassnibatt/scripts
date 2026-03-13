import requests
import csv
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PagerDutyAPI:
    """PagerDuty API client with error handling and rate limiting"""
    
    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token or api_token.strip() == "":
            raise ValueError("API token cannot be empty")
            
        self.base_url = "https://api.pagerduty.com"
        self.headers = {
            "Authorization": f"Token token={api_token.strip()}",
            "Accept": "application/vnd.pagerduty+json;version=2"
        }
        self.min_interval = 1.0 / rate_limit
        self.last_request = 0
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _rate_limit(self):
        """Simple rate limiting"""
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request = time.time()

    def _request(self, url: str, params: dict = None, max_retries: int = 3) -> Optional[requests.Response]:
        """Make API request with retry logic"""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                if response.status_code == 401:
                    logger.error("Authentication failed. Check your API token.")
                    sys.exit(1)
                elif response.status_code == 403:
                    logger.error("Access forbidden. Check token permissions.")
                    sys.exit(1)
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    logger.warning(f"Timeout (attempt {attempt + 1}/{max_retries})")
                    time.sleep(2 ** attempt)
                else:
                    logger.error("Request timed out")
                    return None
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Request failed: {e}")
                    return None
        
        return None

    def validate_token(self) -> bool:
        """Validate API token"""
        logger.info("Validating API token...")
        response = self._request(f"{self.base_url}/users", params={"limit": 1})
        if response and response.status_code == 200:
            logger.info("✓ API token is valid")
            return True
        return False

    def get_incidents(self, start_date: str, end_date: str) -> List[Dict]:
        """Get all incidents within date range"""
        incidents = []
        offset = 0
        limit = 100

        logger.info(f"Fetching incidents from {start_date} to {end_date}...")

        while True:
            params = {
                "since": f"{start_date}T00:00:00Z",
                "until": f"{end_date}T23:59:59Z",
                "offset": offset,
                "limit": limit,
                "include[]": ["first_trigger_log_entry"]
            }

            response = self._request(f"{self.base_url}/incidents", params=params)
            
            if not response:
                logger.error("Failed to fetch incidents")
                break

            data = response.json()
            batch = data.get("incidents", [])
            incidents.extend(batch)
            
            if data.get('more', False):
                logger.info(f"Fetched {len(incidents)} incidents so far...")
                offset += limit
            else:
                logger.info(f"Fetched {len(incidents)} total incidents")
                break

        return incidents

def validate_date(date_str: str) -> bool:
    """Validate date format (YYYY-MM-DD)"""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def extract_incident_data(incidents: List[Dict]) -> List[Dict]:
    """Extract relevant data from incidents"""
    results = []
    
    for incident in incidents:
        trigger_summary = "N/A"
        if incident.get("first_trigger_log_entry"):
            trigger_summary = incident["first_trigger_log_entry"].get("summary", "N/A")

        results.append({
            "incident_id": incident.get("id", "N/A"),
            "incident_number": incident.get("incident_number", "N/A"),
            "incident_title": incident.get("title", "N/A"),
            "status": incident.get("status", "N/A"),
            "urgency": incident.get("urgency", "N/A"),
            "service": incident.get("service", {}).get("summary", "N/A"),
            "created_at": incident.get("created_at", "N/A"),
            "trigger_summary": trigger_summary
        })

    return results

def write_csv(data: List[Dict], filename: str):
    """Write data to CSV file"""
    fieldnames = ["incident_id", "incident_number", "incident_title", "status", 
                  "urgency", "service", "created_at", "trigger_summary"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    logger.info(f"✓ Report saved to '{filename}'")

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Export PagerDuty incidents to CSV')
    parser.add_argument('--start-date', '-s', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', '-e', help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', '-d', type=int, help='Look back N days from today')
    parser.add_argument('--output', '-o', help='Output filename')
    parser.add_argument('--rate-limit', '-r', type=int, default=8, help='Requests per second (default: 8)')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Get API token
    api_token = os.getenv("API_TOKEN", "").strip()
    if not api_token or api_token == "YOUR_PAGERDUTY_API_TOKEN_HERE":
        logger.error("ERROR: Set API_TOKEN environment variable")
        logger.error("  export API_TOKEN='your-token-here'")
        sys.exit(1)

    # Determine date range
    if args.days:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
    else:
        start_str = args.start_date or os.getenv("START_DATE")
        end_str = args.end_date or os.getenv("END_DATE")
        
        # Default to last 7 days
        if not start_str or not end_str:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")
            logger.info(f"Using default: last 7 days ({start_str} to {end_str})")

    # Validate dates
    if not validate_date(start_str) or not validate_date(end_str):
        logger.error("Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)

    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output = args.output or os.getenv("OUTPUT_FILE") or f"pagerduty_incidents_{timestamp}.csv"
    if not output.endswith('.csv'):
        output = f"{output}_{timestamp}.csv"

    try:
        # Initialize API and validate
        api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        if not api.validate_token():
            logger.error("Invalid API token")
            sys.exit(1)

        # Fetch and process incidents
        start_time = time.time()
        incidents = api.get_incidents(start_str, end_str)
        
        if not incidents:
            logger.warning("No incidents found")
            return

        results = extract_incident_data(incidents)
        write_csv(results, output)

        # Summary
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✓ Processed {len(incidents)} incidents in {elapsed:.1f}s")
        print(f"✓ Report saved to '{output}'")
        print(f"{'='*60}\n")

    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()