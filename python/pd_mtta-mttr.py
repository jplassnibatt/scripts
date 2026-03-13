import requests
import csv
from datetime import datetime
import os
from typing import Dict, List
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration Section
API_TOKEN = os.getenv("API_TOKEN", "YOUR_PAGERDUTY_API_TOKEN_HERE")
START_DATE = "2025-07-22"     # Replace with your start date (YYYY-MM-DD)
END_DATE = "2025-07-24"       # Replace with your end date (YYYY-MM-DD)

class PagerDutyExporter:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://api.pagerduty.com"
        self.headers = {
            "Accept": "application/vnd.pagerduty+json;version=2",
            "Authorization": f"Token token={api_token}"
        }

        # Rate limiting configuration
        self.requests_per_second = 4  # PagerDuty's rate limit is 400 requests/minute = ~6.6/second
        self.last_request_time = 0

        # Configure session with retries and timeouts
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _rate_limit(self):
        """Implement rate limiting."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1.0 / self.requests_per_second:
            time.sleep((1.0 / self.requests_per_second) - time_since_last_request)
        self.last_request_time = time.time()

    def _make_request(self, method: str, endpoint: str, params: Dict = None) -> Dict:
        """Make a rate-limited request to the PagerDuty API."""
        self._rate_limit()
        url = f"{self.base_url}/{endpoint}"
        response = self.session.request(method, url, headers=self.headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_incidents(self, start_date: str, end_date: str) -> List[Dict]:
        """Fetch all closed incidents between start_date and end_date."""
        incidents = []
        offset = 0
        limit = 100

        while True:
            params = {
                "since": f"{start_date}T00:00:00Z",
                "until": f"{end_date}T23:59:59Z",
                "statuses[]": "resolved",
                "limit": limit,
                "offset": offset,
                "total": True
            }

            data = self._make_request("GET", "incidents", params)
            incidents.extend(data["incidents"])

            if not data.get("more"):
                break

            offset += limit
            print(f"Fetched {len(incidents)} incidents so far...")

        return incidents

    def get_incident_details(self, incident_id: str) -> Dict:
        """Fetch detailed information about a specific incident."""
        return self._make_request("GET", f"incidents/{incident_id}")["incident"]

    def get_service_details(self, service_id: str) -> Dict:
        """Fetch detailed information about a service."""
        return self._make_request("GET", f"services/{service_id}")["service"]

    def get_incident_log_entries(self, incident_id: str) -> List[Dict]:
        """Fetch all log entries for a specific incident."""
        return self._make_request("GET", f"incidents/{incident_id}/log_entries")["log_entries"]

    def process_single_incident(self, incident: Dict) -> Dict:
        """Process a single incident and return its metrics."""
        try:
            log_entries = self.get_incident_log_entries(incident["id"])
            metrics = self.calculate_time_metrics(incident, log_entries)
            service_name = self.get_service_details(incident["service"]["id"])["name"]

            return {
                'Incident ID': incident["id"],
                'Title': incident["title"],
                'Service Name': service_name,
                'First Acknowledger': metrics["first_acknowledger"],
                'All Acknowledger(s)': metrics["all_acknowledgers"],
                'Resolver': metrics["resolver"],
                'MTTA (HH:MM:SS)': metrics["mtta"],
                'MTTR (HH:MM:SS)': metrics["mttr"]
            }
        except Exception as e:
            print(f"Error processing incident {incident['id']}: {str(e)}")
            return None

    def calculate_time_metrics(self, incident: Dict, log_entries: List[Dict]) -> Dict:
        """Calculate MTTA and MTTR metrics using FIRST acknowledgment time."""
        created_at = datetime.fromisoformat(incident["created_at"].replace('Z', '+00:00'))

        first_ack_time = None
        first_acknowledger = None
        resolve_time = None
        all_acknowledgers = []
        resolver = None

        # Sort log entries by timestamp to ensure chronological order
        sorted_entries = sorted(log_entries, key=lambda x: x["created_at"])

        for entry in sorted_entries:
            if entry["type"] == "acknowledge_log_entry":
                # Only capture the FIRST acknowledgment time and acknowledger
                if first_ack_time is None:
                    first_ack_time = datetime.fromisoformat(entry["created_at"].replace('Z', '+00:00'))
                    first_acknowledger = entry["agent"]["summary"]
                # Still collect all acknowledgers for reporting
                all_acknowledgers.append(entry["agent"]["summary"])
            elif entry["type"] == "resolve_log_entry":
                resolve_time = datetime.fromisoformat(entry["created_at"].replace('Z', '+00:00'))
                resolver = entry["agent"]["summary"]

        mtta = (first_ack_time - created_at) if first_ack_time else None
        mttr = (resolve_time - created_at) if resolve_time else None

        return {
            "first_acknowledger": first_acknowledger if first_acknowledger else "No acknowledgment",
            "all_acknowledgers": ", ".join(set(all_acknowledgers)) if all_acknowledgers else "No acknowledgment",
            "resolver": resolver if resolver else "Not resolved",
            "mtta": self._format_timedelta(mtta) if mtta else "N/A",
            "mttr": self._format_timedelta(mttr) if mttr else "N/A"
        }

    @staticmethod
    def _format_timedelta(td):
        """Format timedelta into hours, minutes, and seconds."""
        if not td:
            return "N/A"

        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def export_incidents_metrics(self, start_date: str, end_date: str):
        """Export metrics for all incidents between start_date and end_date."""
        print(f"Fetching incidents from {start_date} to {end_date}...")
        incidents = self.get_incidents(start_date, end_date)

        if not incidents:
            print("No incidents found for the specified date range.")
            return

        print(f"Found {len(incidents)} incidents. Processing...")

        # Process incidents with concurrent execution
        processed_incidents = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_incident = {executor.submit(self.process_single_incident, incident): incident
                                for incident in incidents}

            completed = 0
            for future in concurrent.futures.as_completed(future_to_incident):
                completed += 1
                print(f"Progress: {completed}/{len(incidents)} incidents processed")
                result = future.result()
                if result:
                    processed_incidents.append(result)

        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"pagerduty_metrics_{timestamp}.csv"

        # Write to CSV
        fieldnames = ['Incident ID', 'Title', 'Service Name',
                     'First Acknowledger', 'All Acknowledger(s)', 'Resolver',
                     'MTTA (HH:MM:SS)', 'MTTR (HH:MM:SS)']

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_incidents)

        print(f"Metrics exported to {filename}")

def validate_date_format(date_str: str) -> bool:
    """Validate if the date string is in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def main():
    try:
        exporter = PagerDutyExporter(API_TOKEN)
        exporter.export_incidents_metrics(START_DATE, END_DATE)
    except requests.exceptions.RequestException as e:
        print(f"Error accessing PagerDuty API: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Validate date formats
    if not validate_date_format(START_DATE) or not validate_date_format(END_DATE):
        print("Error: Dates must be valid and in YYYY-MM-DD format")
        exit(1)

    # Validate date range
    if START_DATE > END_DATE:
        print("Error: Start date must be before or equal to end date")
        exit(1)

    main()
