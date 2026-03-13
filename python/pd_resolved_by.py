import requests
import csv
import os
import sys
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import argparse

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PagerDutyAPI:
    """PagerDuty API client with caching and error handling"""
    
    def __init__(self, api_token: str, rate_limit: int = 8):
        if not api_token:
            raise ValueError("API token cannot be empty")
            
        self.base_url = "https://api.pagerduty.com"
        self.headers = {
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Authorization": f"Token token={api_token.strip()}",
            "Content-Type": "application/json"
        }
        self.user_cache = {}
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
                    wait = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                
                if response.status_code == 401:
                    logger.error("Authentication failed. Check API token.")
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

    def get_user_details(self, user_id: str) -> Optional[Dict]:
        """Fetch user details with caching"""
        if user_id in self.user_cache:
            return self.user_cache[user_id]

        response = self._request(f"{self.base_url}/users/{user_id}")
        
        if response and response.status_code == 200:
            user_data = response.json()["user"]
            self.user_cache[user_id] = {
                "id": user_data["id"],
                "name": user_data["name"],
                "email": user_data["email"]
            }
            return self.user_cache[user_id]
        return None

    def get_resolved_incidents(self, start_date: str, end_date: str, 
                               service_ids: List[str] = None) -> List[Dict]:
        """
        Get resolved incidents within a date range
        
        Args:
            start_date: Start date (YYYY-MM-DD HH:MM:SS)
            end_date: End date (YYYY-MM-DD HH:MM:SS)
            service_ids: Optional list of service IDs to filter
        """
        # Convert to ISO format
        start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        logger.info(f"Fetching resolved incidents from {start_date} to {end_date}")
        if service_ids:
            logger.info(f"Filtering by services: {', '.join(service_ids)}")

        incidents = []
        offset = 0
        limit = 100

        while True:
            params = {
                "statuses[]": "resolved",
                "since": start_dt.isoformat(),
                "until": end_dt.isoformat(),
                "offset": offset,
                "limit": limit,
                "include[]": ["users"]
            }

            if service_ids:
                params["service_ids[]"] = service_ids

            response = self._request(f"{self.base_url}/incidents", params=params)
            
            if not response:
                logger.error("Failed to fetch incidents")
                break

            data = response.json()

            for incident in data.get("incidents", []):
                resolver = None
                resolver_details = None

                # Get resolve log entry
                log_params = {"include[]": ["users"], "is_overview": "true"}
                log_response = self._request(
                    f"{self.base_url}/incidents/{incident['id']}/log_entries",
                    params=log_params
                )

                if log_response and log_response.status_code == 200:
                    log_data = log_response.json()
                    for entry in log_data.get("log_entries", []):
                        if entry.get("type") == "resolve_log_entry":
                            resolver = entry.get("agent", {})
                            if resolver and resolver.get("id"):
                                resolver_details = self.get_user_details(resolver["id"])
                            break

                incident_info = {
                    "incident_id": incident["id"],
                    "incident_number": incident["incident_number"],
                    "title": incident["title"],
                    "created_at": incident["created_at"],
                    "resolved_at": incident["resolved_at"],
                    "resolver": {
                        "id": resolver.get("id") if resolver else None,
                        "name": resolver_details["name"] if resolver_details else resolver.get("summary") if resolver else None,
                        "email": resolver_details["email"] if resolver_details else None
                    } if resolver else None,
                    "urgency": incident["urgency"],
                    "service": incident.get("service", {}).get("summary"),
                    "service_id": incident.get("service", {}).get("id")
                }

                incidents.append(incident_info)
                
                if len(incidents) % 10 == 0:
                    logger.info(f"Processed {len(incidents)} incidents...")

            if not data.get("more"):
                break

            offset += limit

        logger.info(f"✓ Found {len(incidents)} resolved incidents")
        return incidents

def format_datetime(dt_str: str) -> str:
    """Format ISO datetime to readable format"""
    try:
        dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    except:
        return dt_str

def export_to_csv(incidents: List[Dict], filename: str):
    """Export incidents to CSV"""
    fieldnames = [
        'incident_number', 'incident_id', 'title', 'created_at', 'resolved_at',
        'resolver_name', 'resolver_email', 'resolver_id', 'service', 'service_id', 'urgency'
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for incident in incidents:
            writer.writerow({
                'incident_number': incident['incident_number'],
                'incident_id': incident['incident_id'],
                'title': incident['title'],
                'created_at': format_datetime(incident['created_at']),
                'resolved_at': format_datetime(incident['resolved_at']),
                'resolver_name': incident['resolver']['name'] if incident['resolver'] else 'Unknown',
                'resolver_email': incident['resolver']['email'] if incident['resolver'] else 'Unknown',
                'resolver_id': incident['resolver']['id'] if incident['resolver'] else 'Unknown',
                'service': incident['service'],
                'service_id': incident['service_id'],
                'urgency': incident['urgency']
            })
    
    logger.info(f"✓ CSV saved to '{filename}'")

def print_examples():
    """Print usage examples"""
    print("""
USAGE EXAMPLES:

1. Export resolved incidents for last 7 days:
   $ export API_TOKEN='your-token'
   $ python script.py --days 7

2. Specific date range:
   $ python script.py --start "2024-05-01 00:00:00" --end "2024-05-08 23:59:59"

3. Filter by service IDs:
   $ python script.py --days 30 --service-id PXXXXX1 --service-id PXXXXX2

4. Custom output filename:
   $ python script.py --days 30 --output resolved_incidents.csv

5. Last 24 hours:
   $ python script.py --days 1

6. Using environment variables:
   $ export API_TOKEN='your-token'
   $ export START_DATE='2024-05-01 00:00:00'
   $ export END_DATE='2024-05-31 23:59:59'
   $ export SERVICE_IDS='PXXXXX1,PXXXXX2'
   $ python script.py

ENVIRONMENT VARIABLES:
  API_TOKEN      PagerDuty API token (required)
  START_DATE     Start date (YYYY-MM-DD HH:MM:SS)
  END_DATE       End date (YYYY-MM-DD HH:MM:SS)
  SERVICE_IDS    Comma-separated service IDs
  OUTPUT_FILE    Custom output filename
""")

def parse_args():
    parser = argparse.ArgumentParser(
        description='Export resolved PagerDuty incidents with resolver info',
        epilog='Run with --examples for usage examples'
    )
    parser.add_argument('--start', '-s', help='Start date (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--end', '-e', help='End date (YYYY-MM-DD HH:MM:SS)')
    parser.add_argument('--days', '-d', type=int, help='Look back N days from now')
    parser.add_argument('--service-id', action='append', dest='service_ids',
                        help='Service ID to filter (repeatable)')
    parser.add_argument('--output', '-o', help='Output CSV filename')
    parser.add_argument('--rate-limit', '-r', type=int, default=8,
                        help='Requests per second (default: 8)')
    parser.add_argument('--examples', action='store_true', help='Show examples')
    return parser.parse_args()

def main():
    args = parse_args()
    
    if args.examples:
        print_examples()
        sys.exit(0)
    
    # Get API token
    api_token = os.getenv("API_TOKEN", "").strip()
    if not api_token or api_token == "API_TOKEN":
        logger.error("ERROR: Set API_TOKEN environment variable")
        logger.error("Run with --examples for help")
        sys.exit(1)

    # Date range
    if args.days:
        end = datetime.now()
        start = end - timedelta(days=args.days)
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_str = args.start or os.getenv("START_DATE")
        end_str = args.end or os.getenv("END_DATE")
        
        if not start_str or not end_str:
            end = datetime.now()
            start = end - timedelta(days=7)
            start_str = start.strftime("%Y-%m-%d %H:%M:%S")
            end_str = end.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"Using default: last 7 days")

    # Service IDs
    service_ids = args.service_ids or []
    if not service_ids:
        env_services = os.getenv("SERVICE_IDS", "").strip()
        if env_services:
            service_ids = [s.strip() for s in env_services.split(',')]

    # Output filename
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output = args.output or os.getenv("OUTPUT_FILE") or f"pagerduty_incidents_{timestamp}.csv"
    if not output.endswith('.csv'):
        output = f"{output}_{timestamp}.csv"

    try:
        # Fetch incidents
        api = PagerDutyAPI(api_token, rate_limit=args.rate_limit)
        start_time = time.time()
        incidents = api.get_resolved_incidents(start_str, end_str, service_ids)
        
        if not incidents:
            logger.warning("No resolved incidents found")
            return

        # Export to CSV
        export_to_csv(incidents, output)

        # Show sample
        print(f"\nSample (first 3 incidents):")
        print("-" * 70)
        for inc in incidents[:3]:
            print(f"\nIncident #{inc['incident_number']}: {inc['title']}")
            print(f"  Resolved by: {inc['resolver']['name'] if inc['resolver'] else 'Unknown'}")
            print(f"  Service: {inc['service']}")
            print(f"  Urgency: {inc['urgency']}")
        
        if len(incidents) > 3:
            print(f"\n... and {len(incidents) - 3} more incidents")

        # Summary
        elapsed = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"✓ Processed {len(incidents)} incidents in {elapsed:.1f}s")
        print(f"✓ Report saved to '{output}'")
        print(f"{'='*50}\n")

    except KeyboardInterrupt:
        logger.warning("\nInterrupted")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()