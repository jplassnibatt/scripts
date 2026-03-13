#!/usr/bin/env python3
"""
CSE - PagerDuty Terraform Import Generator
Fetches resources from PagerDuty API and generates Terraform import files.

Installation:
    chmod +x <script_name>

Usage:
    ./<script_name>

Or run with Python directly:
    python3 <script_name>

Requisites:
    - Python 3.8+
    - requests library: pip3 install requests
    - Terraform
    - Pagerduty provider >=3.31.4, more info: https://registry.terraform.io/providers/PagerDuty/pagerduty/latest/docs
    - Run 'terraform init' before running the script
"""

import requests
import json
import time
import re
import argparse
import subprocess
import sys
import os
import glob
from typing import Dict, List, Any, Optional, Tuple, Callable
from functools import wraps, lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import logging

# ============================================================================
# CONFIGURATION
# ============================================================================

__version__ = "2.2"

# Get the script name dynamically
SCRIPT_NAME = os.path.basename(sys.argv[0])

API_TOKEN = os.getenv("API_TOKEN", "YOUR_PAGERDUTY_API_TOKEN_HERE")
BASE_URL = "https://api.pagerduty.com"
RATE_LIMIT_DELAY = 0.2  # 200ms between API calls (5 requests/second)
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
MAX_WORKERS = 5  # For parallel processing

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ResourceConfig:
    """Configuration for a PagerDuty resource type"""
    terraform_type: str
    naming_field: str
    output_file: str
    api_endpoint: Optional[str] = None
    resource_key: Optional[str] = None
    requires_sub_fetch: bool = False
    sub_fetch_function: Optional[str] = None
    requires_expansion: bool = False
    expansion_field: Optional[str] = None
    id_format: Optional[str] = None
    skip_sanitization: bool = False
    filter_function: Optional[Callable] = None

# ============================================================================
# GLOBAL CACHE & RATE LIMITING
# ============================================================================

class APICache:
    """Thread-safe API cache with statistics"""
    def __init__(self):
        self._cache = {}
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            self.hits += 1
            return self._cache[key]
        self.misses += 1
        return None
    
    def set(self, key: str, value: Any):
        self._cache[key] = value
    
    def clear(self):
        self._cache.clear()
        self.hits = 0
        self.misses = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0

api_cache = APICache()

class RateLimiter:
    """Simple rate limiter"""
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = 0
    
    def wait(self):
        current_time = time.time()
        time_since_last = current_time - self.last_call
        if time_since_last < self.delay:
            time.sleep(self.delay - time_since_last)
        self.last_call = time.time()

rate_limiter = RateLimiter(RATE_LIMIT_DELAY)

# ============================================================================
# API HELPER FUNCTIONS
# ============================================================================

def make_api_request(endpoint: str, params: Optional[Dict] = None) -> Dict:
    """
    Make a GET request to PagerDuty API with retry logic and caching
    """
    # Create cache key
    cache_key = f"{endpoint}:{json.dumps(params, sort_keys=True)}"
    
    # Check cache
    cached_result = api_cache.get(cache_key)
    if cached_result is not None:
        logger.debug(f"[CACHE HIT] {endpoint}")
        return cached_result
    
    # Rate limiting
    rate_limiter.wait()
    
    url = f"{BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Token token={API_TOKEN}",
        "Accept": "application/vnd.pagerduty+json;version=2",
        "Content-Type": "application/json"
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.debug(f"[API CALL] GET {endpoint} (attempt {attempt + 1})")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 429:  # Rate limited
                retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                logger.warning(f"[RATE LIMIT] Waiting {retry_after}s before retry...")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            result = response.json()
            
            # Cache the result
            api_cache.set(cache_key, result)
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"[ERROR] Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"[FATAL] Failed after {MAX_RETRIES} attempts")
                return {}
    
    return {}

def fetch_paginated_resources(endpoint: str, resource_key: str, params: Optional[Dict] = None) -> List[Dict]:
    """
    Fetch all resources from a paginated API endpoint
    """
    all_resources = []
    offset = 0
    limit = 100
    
    if params is None:
        params = {}
    
    while True:
        params.update({"offset": offset, "limit": limit})
        data = make_api_request(endpoint, params)
        
        if not data or resource_key not in data:
            break
        
        resources = data[resource_key]
        if not resources:
            break
        
        all_resources.extend(resources)
        
        if not data.get("more", False):
            break
        
        offset += limit
    
    return all_resources

# ============================================================================
# RESOURCE-SPECIFIC FETCH FUNCTIONS (Optimized with parallel processing)
# ============================================================================

def fetch_business_service_subscribers(business_services: List[Dict]) -> List[Dict]:
    """Fetch subscribers for all business services (parallel)"""
    all_subscribers = []
    
    def fetch_for_service(bs: Dict) -> List[Dict]:
        bs_id = bs["id"]
        bs_name = bs.get("name", bs_id)
        logger.info(f"  Fetching subscribers for: {bs_name}")
        
        subscribers = fetch_paginated_resources(
            f"/business_services/{bs_id}/subscribers",
            "subscribers"
        )
        
        for sub in subscribers:
            sub["_business_service_id"] = bs_id
            sub["_business_service_name"] = bs_name
        
        return subscribers
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_service, bs) for bs in business_services]
        for future in as_completed(futures):
            try:
                all_subscribers.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching subscribers: {e}")
    
    return all_subscribers

def fetch_jira_cloud_account_mapping_rules() -> List[Dict]:
    """Fetch rules for all Jira Cloud account mappings"""
    all_rules = []
    
    logger.info("  Fetching Jira Cloud account mappings...")
    account_mappings = fetch_paginated_resources(
        "/integration-jira-cloud/accounts_mappings",
        "accounts_mappings"
    )
    
    logger.info(f"  Found {len(account_mappings)} Jira Cloud account mappings")
    
    def fetch_rules_for_mapping(mapping: Dict) -> List[Dict]:
        mapping_id = mapping["id"]
        logger.info(f"  Fetching rules for mapping: {mapping_id}")
        
        rules = fetch_paginated_resources(
            f"/integration-jira-cloud/accounts_mappings/{mapping_id}/rules",
            "rules"
        )
        
        for rule in rules:
            rule["_account_mapping_id"] = mapping_id
        
        return rules
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_rules_for_mapping, m) for m in account_mappings]
        for future in as_completed(futures):
            try:
                all_rules.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching Jira rules: {e}")
    
    return all_rules

def fetch_service_integrations(services: List[Dict]) -> List[Dict]:
    """Fetch integrations for all services (parallel)"""
    all_integrations = []
    
    def fetch_for_service(service: Dict) -> List[Dict]:
        service_id = service["id"]
        service_name = service.get("name", service_id)
        integrations = []
        
        integration_refs = service.get("integrations", [])
        
        for integration_ref in integration_refs:
            integration_id = integration_ref["id"]
            data = make_api_request(f"/services/{service_id}/integrations/{integration_id}")
            
            if data and "integration" in data:
                integration = data["integration"]
                integration["_service_id"] = service_id
                integration["_service_name"] = service_name
                integrations.append(integration)
        
        return integrations
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_service, s) for s in services]
        for future in as_completed(futures):
            try:
                all_integrations.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching service integrations: {e}")
    
    return all_integrations

def fetch_team_memberships(teams: List[Dict]) -> List[Dict]:
    """Fetch memberships for all teams (parallel)"""
    all_memberships = []
    
    def fetch_for_team(team: Dict) -> List[Dict]:
        team_id = team["id"]
        team_name = team.get("name", team_id)
        logger.info(f"  Fetching members for team: {team_name}")
        
        members = fetch_paginated_resources(
            f"/teams/{team_id}/members",
            "members"
        )
        
        for member in members:
            member["_team_id"] = team_id
            member["_team_name"] = team_name
        
        return members
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_team, t) for t in teams]
        for future in as_completed(futures):
            try:
                all_memberships.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching team memberships: {e}")
    
    return all_memberships

def fetch_automation_actions_action_service_associations(actions: List[Dict]) -> List[Dict]:
    """Fetch service associations for all automation actions (parallel)"""
    all_associations = []
    
    def fetch_for_action(action: Dict) -> List[Dict]:
        action_id = action["id"]
        action_desc = action.get("description", action_id)
        
        associations = fetch_paginated_resources(
            f"/automation_actions/actions/{action_id}/services",
            "services"
        )
        
        for assoc in associations:
            assoc["_action_id"] = action_id
            assoc["_action_description"] = action_desc
        
        return associations
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_action, a) for a in actions]
        for future in as_completed(futures):
            try:
                all_associations.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching action service associations: {e}")
    
    return all_associations

def fetch_automation_actions_action_team_associations(actions: List[Dict]) -> List[Dict]:
    """Fetch team associations for all automation actions (parallel)"""
    all_associations = []
    
    def fetch_for_action(action: Dict) -> List[Dict]:
        action_id = action["id"]
        action_desc = action.get("description", action_id)
        
        associations = fetch_paginated_resources(
            f"/automation_actions/actions/{action_id}/teams",
            "teams"
        )
        
        for assoc in associations:
            assoc["_action_id"] = action_id
            assoc["_action_description"] = action_desc
        
        return associations
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_action, a) for a in actions]
        for future in as_completed(futures):
            try:
                all_associations.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching action team associations: {e}")
    
    return all_associations

def fetch_automation_actions_runner_team_associations(runners: List[Dict]) -> List[Dict]:
    """Fetch team associations for runbook runners"""
    all_associations = []
    
    logger.info("  Fetching all runners for team associations...")
    all_runners = fetch_paginated_resources(
        "/automation_actions/runners",
        "runners"
    )
    
    runbook_runners = [r for r in all_runners if r.get("runner_type") == "runbook"]
    logger.info(f"  Found {len(runbook_runners)} runbook runners")
    
    for runner in runbook_runners:
        runner_id = runner["id"]
        teams = runner.get("teams", [])
        
        for team in teams:
            all_associations.append({
                "_runner_id": runner_id,
                "id": team["id"]
            })
    
    return all_associations

def fetch_service_event_rules(services: List[Dict]) -> List[Dict]:
    """Fetch event rules for all services (parallel)"""
    all_rules = []
    
    def fetch_for_service(service: Dict) -> List[Dict]:
        service_id = service["id"]
        service_name = service.get("name", service_id)
        
        data = make_api_request(f"/services/{service_id}/rules")
        
        rules = []
        if data and "rules" in data:
            for rule in data["rules"]:
                rule["_service_id"] = service_id
                rule["_service_name"] = service_name
                rules.append(rule)
        
        return rules
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_service, s) for s in services]
        for future in as_completed(futures):
            try:
                all_rules.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching service event rules: {e}")
    
    return all_rules

def fetch_tag_assignments(tags: List[Dict]) -> List[Dict]:
    """Fetch assignments for each tag (parallel)"""
    all_assignments = []
    entity_types = ['users', 'teams', 'escalation_policies']
    
    def fetch_for_tag_and_type(tag_id: str, entity_type: str) -> List[Dict]:
        entities = fetch_paginated_resources(
            f"/tags/{tag_id}/{entity_type}",
            entity_type
        )
        
        assignments = []
        for entity in entities:
            assignments.append({
                'tag_id': tag_id,
                'entity_type': entity_type,
                'entity_id': entity['id'],
                'id': f"{entity_type}.{entity['id']}.{tag_id}"
            })
        
        return assignments
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for tag in tags:
            for entity_type in entity_types:
                futures.append(executor.submit(fetch_for_tag_and_type, tag['id'], entity_type))
        
        for future in as_completed(futures):
            try:
                all_assignments.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching tag assignments: {e}")
    
    return all_assignments

def fetch_enablements() -> List[Dict]:
    """Fetch enablements for all services and event orchestrations (parallel)"""
    all_enablements = []
    
    # Fetch service enablements
    logger.info("  Fetching services for enablements...")
    services = fetch_paginated_resources("/services", "services")
    logger.info(f"  Found {len(services)} services")
    
    # Fetch event orchestration enablements
    logger.info("  Fetching event orchestrations for enablements...")
    orchestrations = fetch_paginated_resources("/event_orchestrations", "orchestrations")
    logger.info(f"  Found {len(orchestrations)} event orchestrations")
    
    def fetch_service_enablements(service: Dict) -> List[Dict]:
        service_id = service["id"]
        service_name = service.get("name", service_id)
        
        data = make_api_request(f"/services/{service_id}/enablements")
        
        enablements = []
        if data and "enablements" in data:
            for enablement in data["enablements"]:
                if enablement.get("enabled", False):  # Only include enabled features
                    enablement["_parent_type"] = "service"
                    enablement["_parent_id"] = service_id
                    enablement["_parent_name"] = service_name
                    enablements.append(enablement)
        
        return enablements
    
    def fetch_orchestration_enablements(orchestration: Dict) -> List[Dict]:
        orch_id = orchestration["id"]
        orch_name = orchestration.get("name", orch_id)
        
        data = make_api_request(f"/event_orchestrations/{orch_id}/enablements")
        
        enablements = []
        if data and "enablements" in data:
            for enablement in data["enablements"]:
                if enablement.get("enabled", False):  # Only include enabled features
                    enablement["_parent_type"] = "event_orchestration"
                    enablement["_parent_id"] = orch_id
                    enablement["_parent_name"] = orch_name
                    enablements.append(enablement)
        
        return enablements
    
    # Parallel fetch for services
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_service_enablements, s) for s in services]
        for future in as_completed(futures):
            try:
                all_enablements.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching service enablements: {e}")
    
    # Parallel fetch for event orchestrations
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_orchestration_enablements, o) for o in orchestrations]
        for future in as_completed(futures):
            try:
                all_enablements.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching orchestration enablements: {e}")
    
    logger.info(f"  Total enablements found: {len(all_enablements)}")
    return all_enablements

def fetch_event_orchestration_integrations(orchestrations: List[Dict]) -> List[Dict]:
    """Fetch integrations for each event orchestration (parallel)"""
    all_integrations = []
    
    def fetch_for_orchestration(orchestration: Dict) -> List[Dict]:
        orchestration_id = orchestration['id']
        orchestration_name = orchestration.get('name', orchestration_id)
        
        data = make_api_request(f"/event_orchestrations/{orchestration_id}/integrations")
        
        integrations = []
        if data and 'integrations' in data:
            for integration in data['integrations']:
                integration['_orchestration_id'] = orchestration_id
                integration['integration_id'] = integration['id']
                integrations.append(integration)
        
        return integrations
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_orchestration, o) for o in orchestrations]
        for future in as_completed(futures):
            try:
                all_integrations.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching orchestration integrations: {e}")
    
    return all_integrations

def fetch_event_orchestration_global_cache_variables(orchestrations: List[Dict]) -> List[Dict]:
    """Fetch global cache variables for event orchestrations (parallel)"""
    all_variables = []
    
    def fetch_for_orchestration(orch: Dict) -> List[Dict]:
        orch_id = orch["id"]
        orch_name = orch.get("name", orch_id)
        logger.info(f"  Fetching global cache variables for: {orch_name}")
        
        try:
            data = make_api_request(f"/event_orchestrations/{orch_id}/cache_variables")
            variables = data.get("cache_variables", [])
            
            for variable in variables:
                variable["_orchestration_id"] = orch_id
            
            return variables
        except Exception as e:
            logger.debug(f"  No cache variables for orchestration {orch_name}: {e}")
            return []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_orchestration, orch) for orch in orchestrations]
        for future in as_completed(futures):
            try:
                all_variables.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching global cache variables: {e}")
    
    return all_variables

def fetch_event_orchestration_service_cache_variables(services: List[Dict]) -> List[Dict]:
    """Fetch service cache variables for all services (parallel)"""
    all_variables = []
    
    def fetch_for_service(service: Dict) -> List[Dict]:
        service_id = service["id"]
        service_name = service.get("name", service_id)
        logger.info(f"  Fetching service cache variables for: {service_name}")
        
        try:
            data = make_api_request(f"/event_orchestrations/services/{service_id}/cache_variables")
            variables = data.get("cache_variables", [])
            
            for variable in variables:
                variable["_service_id"] = service_id
            
            return variables
        except Exception as e:
            logger.debug(f"  No cache variables for service {service_name}: {e}")
            return []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_service, service) for service in services]
        for future in as_completed(futures):
            try:
                all_variables.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching service cache variables: {e}")
    
    return all_variables

def fetch_incident_type_custom_fields(incident_types: List[Dict]) -> List[Dict]:
    """Fetch custom fields for incident types (parallel)"""
    all_fields = []
    
    def fetch_for_incident_type(incident_type: Dict) -> List[Dict]:
        incident_type_id = incident_type["id"]
        incident_type_name = incident_type.get("display_name", incident_type_id)
        logger.info(f"  Fetching custom fields for incident type: {incident_type_name}")
        
        try:
            data = make_api_request(f"/incidents/types/{incident_type_id}/custom_fields")
            fields = data.get("fields", [])
            
            for field in fields:
                field["_incident_type_id"] = incident_type_id
            
            return fields
        except Exception as e:
            logger.debug(f"  No custom fields for incident type {incident_type_name}: {e}")
            return []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_incident_type, it) for it in incident_types]
        for future in as_completed(futures):
            try:
                all_fields.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching incident type custom fields: {e}")
    
    return all_fields

def fetch_user_handoff_notification_rules(users: List[Dict]) -> List[Dict]:
    """Fetch handoff notification rules for all users (parallel)"""
    all_rules = []
    
    def fetch_for_user(user: Dict) -> List[Dict]:
        user_id = user["id"]
        user_name = user.get("name", user_id)
        logger.info(f"  Fetching handoff notification rules for: {user_name}")
        
        try:
            data = make_api_request(f"/users/{user_id}/oncall_handoff_notification_rules")
            rules = data.get("oncall_handoff_notification_rules", [])
            
            for rule in rules:
                rule["_user_id"] = user_id
            
            return rules
        except Exception as e:
            logger.debug(f"  No handoff notification rules for user {user_name}: {e}")
            return []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_for_user, user) for user in users]
        for future in as_completed(futures):
            try:
                all_rules.extend(future.result())
            except Exception as e:
                logger.error(f"Error fetching user handoff notification rules: {e}")
    
    return all_rules

def fetch_incident_workflow_triggers() -> List[Dict]:
    """Fetch incident workflow triggers (no pagination support)"""
    logger.info("  Fetching incident workflow triggers...")
    
    data = make_api_request("/incident_workflows/triggers")
    
    if data and "triggers" in data:
        triggers = data["triggers"]
        logger.info(f"  Found {len(triggers)} incident workflow triggers")
        return triggers
    
    return []

def fetch_service_custom_field_values(services: List[Dict]) -> List[Dict]:
    """Return services as-is for custom field value import (no additional API calls)"""
    logger.info(f"  Using {len(services)} existing services for custom field values")
    return services


# ============================================================================
# RESOURCE CONFIGURATION (Using dataclass)
# ============================================================================

RESOURCE_CONFIGS = {
    "pagerduty_addon": ResourceConfig(
        api_endpoint="/addons",
        resource_key="addons",
        terraform_type="pagerduty_addon",
        naming_field="id",
        output_file="addons",
    ),
    "pagerduty_extension": ResourceConfig(
        api_endpoint="/extensions",
        resource_key="extensions",
        terraform_type="pagerduty_extension",
        naming_field="id",
        output_file="extensions",
    ),
    "pagerduty_service": ResourceConfig(
        api_endpoint="/services",
        resource_key="services",
        terraform_type="pagerduty_service",
        naming_field="id",
        output_file="services",
    ),
    "pagerduty_team": ResourceConfig(
        api_endpoint="/teams",
        resource_key="teams",
        terraform_type="pagerduty_team",
        naming_field="id",
        output_file="teams",
    ),
    "pagerduty_user": ResourceConfig(
        api_endpoint="/users",
        resource_key="users",
        terraform_type="pagerduty_user",
        naming_field="id",
        output_file="users",
    ),
    "pagerduty_schedule": ResourceConfig(
        api_endpoint="/schedules",
        resource_key="schedules",
        terraform_type="pagerduty_schedule",
        naming_field="id",
        output_file="schedules",
    ),
    "pagerduty_business_service": ResourceConfig(
        api_endpoint="/business_services",
        resource_key="business_services",
        terraform_type="pagerduty_business_service",
        naming_field="id",
        output_file="business_services",
    ),
    "pagerduty_alert_grouping_setting": ResourceConfig(
        api_endpoint="/alert_grouping_settings",
        resource_key="alert_grouping_settings",
        terraform_type="pagerduty_alert_grouping_setting",
        naming_field="id",
        output_file="alert_grouping_settings",
    ),
    "pagerduty_business_service_subscriber": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_business_service_subscribers",
        terraform_type="pagerduty_business_service_subscriber",
        naming_field="_business_service_id+subscriber_id",
        output_file="business_service_subscribers",
        id_format="{_business_service_id}.{subscriber_type}.{subscriber_id}",
        skip_sanitization=True,
    ),
    "pagerduty_jira_cloud_account_mapping_rule": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_jira_cloud_account_mapping_rules",
        terraform_type="pagerduty_jira_cloud_account_mapping_rule",
        naming_field="_account_mapping_id+id",
        output_file="jira_cloud_account_mapping_rules",
        id_format="{_account_mapping_id}:{id}",
        skip_sanitization=True,
    ),
    "pagerduty_service_integration": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_service_integrations",
        terraform_type="pagerduty_service_integration",
        naming_field="_service_id+id",
        output_file="service_integrations",
        id_format="{_service_id}.{id}",
        skip_sanitization=True,
    ),
    "pagerduty_team_membership": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_team_memberships",
        terraform_type="pagerduty_team_membership",
        naming_field="user.id+_team_id",
        output_file="team_memberships",
        id_format="{user.id}:{_team_id}",
        skip_sanitization=True,
    ),
    "pagerduty_user_contact_method": ResourceConfig(
        api_endpoint="/users",
        resource_key="users",
        terraform_type="pagerduty_user_contact_method",
        naming_field="id",
        output_file="user_contact_methods",
        requires_expansion=True,
        expansion_field="contact_methods",
        id_format="{_user_id}:{id}",
    ),
    "pagerduty_user_notification_rule": ResourceConfig(
        api_endpoint="/users",
        resource_key="users",
        terraform_type="pagerduty_user_notification_rule",
        naming_field="id",
        output_file="user_notification_rules",
        requires_expansion=True,
        expansion_field="notification_rules",
        id_format="{_user_id}:{id}",
    ),
    "pagerduty_automation_actions_action": ResourceConfig(
        api_endpoint="/automation_actions/actions",
        resource_key="actions",
        terraform_type="pagerduty_automation_actions_action",
        naming_field="id",
        output_file="automation_actions_actions",
    ),
    "pagerduty_automation_actions_action_service_association": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_automation_actions_action_service_associations",
        terraform_type="pagerduty_automation_actions_action_service_association",
        naming_field="id",
        output_file="automation_actions_action_service_associations",
        id_format="{_action_id}:{id}",
    ),
    "pagerduty_automation_actions_action_team_association": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_automation_actions_action_team_associations",
        terraform_type="pagerduty_automation_actions_action_team_association",
        naming_field="id",
        output_file="automation_actions_action_team_associations",
        id_format="{_action_id}:{id}",
    ),
    "pagerduty_enablement": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_enablements",
        terraform_type="pagerduty_enablement",
        naming_field="_parent_id+feature",
        output_file="enablements",
        id_format="{_parent_type}.{_parent_id}.{feature}",
        skip_sanitization=True,
    ),
    "pagerduty_event_orchestration": ResourceConfig(
        api_endpoint="/event_orchestrations",
        resource_key="orchestrations",
        terraform_type="pagerduty_event_orchestration",
        naming_field="id",
        output_file="event_orchestrations",
    ),
    "pagerduty_event_orchestration_global": ResourceConfig(
        api_endpoint="/event_orchestrations",
        resource_key="orchestrations",
        terraform_type="pagerduty_event_orchestration_global",
        naming_field="id",
        output_file="event_orchestration_globals",
    ),
    "pagerduty_event_orchestration_integration": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_event_orchestration_integrations",
        terraform_type="pagerduty_event_orchestration_integration",
        naming_field="_orchestration_id+integration_id",
        output_file="event_orchestration_integrations",
        id_format="{_orchestration_id}:{integration_id}",
        skip_sanitization=True,
    ),
    "pagerduty_event_orchestration_router": ResourceConfig(
        api_endpoint="/event_orchestrations",
        resource_key="orchestrations",
        terraform_type="pagerduty_event_orchestration_router",
        naming_field="id",
        output_file="event_orchestration_routers",
    ),
    "pagerduty_event_orchestration_service": ResourceConfig(
        api_endpoint="/services",
        resource_key="services",
        terraform_type="pagerduty_event_orchestration_service",
        naming_field="id",
        output_file="event_orchestration_services",
    ),
    "pagerduty_event_orchestration_unrouted": ResourceConfig(
        api_endpoint="/event_orchestrations",
        resource_key="orchestrations",
        terraform_type="pagerduty_event_orchestration_unrouted",
        naming_field="id",
        output_file="event_orchestration_unrouteds",
    ),
    "pagerduty_event_orchestration_global_cache_variable": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_event_orchestration_global_cache_variables",
        terraform_type="pagerduty_event_orchestration_global_cache_variable",
        naming_field="_orchestration_id+id",
        output_file="event_orchestration_global_cache_variables",
        id_format="{_orchestration_id}:{id}",
        skip_sanitization=True,
    ),
    "pagerduty_event_orchestration_service_cache_variable": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_event_orchestration_service_cache_variables",
        terraform_type="pagerduty_event_orchestration_service_cache_variable",
        naming_field="_service_id+id",
        output_file="event_orchestration_service_cache_variables",
        id_format="{_service_id}:{id}",
        skip_sanitization=True,
    ),
    "pagerduty_service_event_rule": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_service_event_rules",
        terraform_type="pagerduty_service_event_rule",
        naming_field="id",
        output_file="service_event_rules",
        id_format="{_service_id}.{id}",
    ),
    "pagerduty_tag": ResourceConfig(
        api_endpoint="/tags",
        resource_key="tags",
        terraform_type="pagerduty_tag",
        naming_field="id",
        output_file="tags",
        skip_sanitization=True,
    ),
    "pagerduty_tag_assignment": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_tag_assignments",
        terraform_type="pagerduty_tag_assignment",
        naming_field="entity_type+tag_id+entity_id",
        output_file="tag_assignments",
        id_format="{entity_type}.{entity_id}.{tag_id}",
        skip_sanitization=True,
    ),
    "pagerduty_automation_actions_runner": ResourceConfig(
        api_endpoint="/automation_actions/runners",
        resource_key="runners",
        terraform_type="pagerduty_automation_actions_runner",
        naming_field="id",
        output_file="automation_actions_runners",
        filter_function=lambda r: r.get("runner_type") == "runbook",
    ),
    "pagerduty_automation_actions_runner_team_association": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_automation_actions_runner_team_associations",
        terraform_type="pagerduty_automation_actions_runner_team_association",
        naming_field="_runner_id+id",
        output_file="automation_actions_runner_team_associations",
        id_format="{_runner_id}:{id}",
        skip_sanitization=True,
    ),
    "pagerduty_escalation_policy": ResourceConfig(
        api_endpoint="/escalation_policies",
        resource_key="escalation_policies",
        terraform_type="pagerduty_escalation_policy",
        naming_field="id",
        output_file="escalation_policies",
    ),
    "pagerduty_incident_type": ResourceConfig(
        api_endpoint="/incidents/types",
        resource_key="incident_types",
        terraform_type="pagerduty_incident_type",
        naming_field="id",
        output_file="incident_types",
        filter_function=lambda r: not r.get("name", "").endswith("_default"),
    ),
    "pagerduty_incident_type_custom_field": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_incident_type_custom_fields",
        terraform_type="pagerduty_incident_type_custom_field",
        naming_field="_incident_type_id+id",
        output_file="incident_type_custom_fields",
        id_format="{_incident_type_id}:{id}",
        skip_sanitization=True,
    ),
    "pagerduty_incident_workflow": ResourceConfig(
        api_endpoint="/incident_workflows",
        resource_key="incident_workflows",
        terraform_type="pagerduty_incident_workflow",
        naming_field="id",
        output_file="incident_workflows",
    ),
    "pagerduty_incident_workflow_trigger": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_incident_workflow_triggers",
        terraform_type="pagerduty_incident_workflow_trigger",
        naming_field="id",
        output_file="incident_workflow_triggers",
    ),
    "pagerduty_user_handoff_notification_rule": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_user_handoff_notification_rules",
        terraform_type="pagerduty_user_handoff_notification_rule",
        naming_field="_user_id+id",
        output_file="user_handoff_notification_rules",
        id_format="{_user_id}.{id}",
        skip_sanitization=False,
    ),
    "pagerduty_webhook_subscription": ResourceConfig(
        api_endpoint="/webhook_subscriptions",
        resource_key="webhook_subscriptions",
        terraform_type="pagerduty_webhook_subscription",
        naming_field="id",
        output_file="webhook_subscriptions",
    ),
    "pagerduty_service_custom_field": ResourceConfig(
        api_endpoint="/services/custom_fields",
        resource_key="fields",
        terraform_type="pagerduty_service_custom_field",
        naming_field="id",
        output_file="service_custom_fields",
    ),
    "pagerduty_service_custom_field_value": ResourceConfig(
        requires_sub_fetch=True,
        sub_fetch_function="fetch_service_custom_field_values",
        terraform_type="pagerduty_service_custom_field_value",
        naming_field="id",
        output_file="service_custom_field_values",
    ),
    }

# ============================================================================
# RESOURCE DEPENDENCIES
# ============================================================================

RESOURCE_DEPENDENCIES = {
    "pagerduty_business_service_subscriber": ["pagerduty_business_service"],
    "pagerduty_service_integration": ["pagerduty_service"],
    "pagerduty_team_membership": ["pagerduty_team"],
    "pagerduty_user_contact_method": ["pagerduty_user"],
    "pagerduty_user_notification_rule": ["pagerduty_user"],
    "pagerduty_automation_actions_action_service_association": ["pagerduty_automation_actions_action"],
    "pagerduty_automation_actions_action_team_association": ["pagerduty_automation_actions_action"],
    "pagerduty_automation_actions_runner_team_association": ["pagerduty_automation_actions_runner"],
    "pagerduty_service_event_rule": ["pagerduty_service"],
    "pagerduty_tag_assignment": ["pagerduty_tag"],
    "pagerduty_event_orchestration_integration": ["pagerduty_event_orchestration"],
    "pagerduty_event_orchestration_global": ["pagerduty_event_orchestration"],
    "pagerduty_event_orchestration_router": ["pagerduty_event_orchestration"],
    "pagerduty_event_orchestration_service": ["pagerduty_service"],
    "pagerduty_event_orchestration_unrouted": ["pagerduty_event_orchestration"],
    "pagerduty_event_orchestration_global_cache_variable": ["pagerduty_event_orchestration"],
    "pagerduty_event_orchestration_service_cache_variable": ["pagerduty_service"],
    "pagerduty_incident_type_custom_field": ["pagerduty_incident_type"],
    "pagerduty_user_handoff_notification_rule": ["pagerduty_user"],
    "pagerduty_service_custom_field_value": ["pagerduty_service"],
}

# ============================================================================
# DYNAMIC LINE NUMBER DETECTION
# ============================================================================

def get_api_token_line_number():
    """Find the line number where API_TOKEN is defined"""
    try:
        with open(__file__, 'r') as f:
            for line_num, line in enumerate(f, start=1):
                if 'API_TOKEN = os.getenv(' in line:
                    return line_num
    except:
        pass
    return None

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@lru_cache(maxsize=1024)
def get_nested_value(obj_json: str, path: str) -> Any:
    """Get nested value from dictionary using dot notation (cached)"""
    obj = json.loads(obj_json)
    keys = path.split('.')
    value = obj
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key, '')
        else:
            return ''
    return value

@lru_cache(maxsize=512)
def sanitize_name(name: str) -> str:
    """Sanitize resource name for Terraform (cached)"""
    if not name:
        return "unnamed"
    
    sanitized = name.lower()
    sanitized = ''.join(c if c.isalnum() else '_' for c in sanitized)
    sanitized = '_'.join(filter(None, sanitized.split('_')))
    
    if sanitized and not sanitized[0].isalpha():
        sanitized = 'r_' + sanitized
    
    return sanitized or "unnamed"

def generate_resource_name(resource: Dict, naming_field: str, skip_sanitization: bool = False) -> str:
    """Generate Terraform resource name from resource data"""
    if naming_field == "id":
        resource_id = resource.get("id", "unnamed")
        if resource_id and not resource_id[0].isalpha():
            resource_id = 'r_' + resource_id
        return resource_id
    
    if '+' in naming_field:
        parts = naming_field.split('+')
        name_parts = []
        resource_json = json.dumps(resource, sort_keys=True)
        
        for part in parts:
            value = get_nested_value(resource_json, part)
            if value:
                name_parts.append(str(value))
        
        combined_name = '_'.join(name_parts)
        
        if skip_sanitization:
            if combined_name and not combined_name[0].isalpha():
                combined_name = 'r_' + combined_name
            return combined_name
        else:
            return sanitize_name(combined_name)
    
    resource_json = json.dumps(resource, sort_keys=True)
    value = get_nested_value(resource_json, naming_field)
    
    if skip_sanitization:
        value_str = str(value)
        if value_str and not value_str[0].isalpha():
            value_str = 'r_' + value_str
        return value_str
    else:
        return sanitize_name(str(value))

def format_import_id(resource: Dict, id_format: str) -> str:
    """Format the import ID using the specified format string"""
    if not id_format:
        return resource.get("id", "")
    
    import_id = id_format
    resource_json = json.dumps(resource, sort_keys=True)
    placeholders = re.findall(r'\{([^}]+)\}', id_format)
    
    for placeholder in placeholders:
        value = get_nested_value(resource_json, placeholder)
        import_id = import_id.replace(f"{{{placeholder}}}", str(value))
    
    return import_id

def generate_import_file_for_resource(resource_type: str, config: ResourceConfig, resources: List[Dict]) -> Dict:
    """Generate Terraform import file for a specific resource type"""
    import_blocks = []
    resource_names_count = {}
    target_resources = []
    skipped_count = 0
    
    for resource in resources:
        resource_name = generate_resource_name(resource, config.naming_field, config.skip_sanitization)
        
        if resource_name in resource_names_count:
            resource_names_count[resource_name] += 1
            resource_name = f"{resource_name}_{resource_names_count[resource_name]}"
        else:
            resource_names_count[resource_name] = 1
        
        import_id = format_import_id(resource, config.id_format) if config.id_format else resource.get("id", "")
        
        # Skip owner users
        if config.terraform_type == "pagerduty_user" and resource.get("role") == "owner":
            import_blocks.append(f"# SKIPPED: Owner user cannot be imported\n# import {{\n#   id = \"{import_id}\"\n#   to = {config.terraform_type}.{resource_name}\n# }}\n")
            skipped_count += 1
            continue
        
        import_block = f'import {{\n  id = "{import_id}"\n  to = {config.terraform_type}.{resource_name}\n}}\n'
        import_blocks.append(import_block)
        target_resources.append(f"{config.terraform_type}.{resource_name}")
    
    # Write to .tf file
    tf_filename = f"to_import_{config.output_file}.tf"
    
    actual_count = len(target_resources)
    
    with open(tf_filename, 'w') as f:
        f.write(f"# Terraform import file for {config.terraform_type}\n")
        f.write(f"# Generated resources: {actual_count}\n\n")
        f.write('\n'.join(import_blocks))
    
    logger.info(f"✓ Generated {tf_filename} with {actual_count} resources")
    
    return {
        "output_file": config.output_file,
        "terraform_type": config.terraform_type,
        "targets": target_resources
    }

def resolve_dependencies(selected_resources: List[str]) -> List[str]:
    """Resolve and add missing dependencies for selected resources"""
    resolved = set(selected_resources)
    added_dependencies = set()
    
    for resource in selected_resources:
        if resource in RESOURCE_DEPENDENCIES:
            for dependency in RESOURCE_DEPENDENCIES[resource]:
                if dependency not in resolved:
                    resolved.add(dependency)
                    added_dependencies.add(dependency)
    
    if added_dependencies:
        logger.info("\n" + "="*80)
        logger.info("Auto-added dependencies:")
        logger.info("="*80)
        for dep in sorted(added_dependencies):
            logger.info(f"  + {dep}")
        logger.info("="*80)
    
    # Sort to maintain a logical order (dependencies first)
    all_resources = list(RESOURCE_CONFIGS.keys())
    return [r for r in all_resources if r in resolved]

# ============================================================================
# RESOURCE SELECTION MENU
# ============================================================================

def display_resource_menu() -> List[str]:
    """Display interactive menu for resource selection"""
    resource_list = list(RESOURCE_CONFIGS.keys())
    
    # Sort alphabetically for display
    sorted_resource_list = sorted(resource_list)
    
    print("\n" + "="*80)
    print("PagerDuty Resource Selection Menu")
    print("="*80)
    print("\nSelect resources to import:")
    print("\nOptions:")
    print("  - Enter numbers separated by commas: 1,3,5")
    print("  - Enter ranges: 1-10")
    print("  - Enter 0 or press Enter to import all resources")
    print("  - Enter 'q' to quit")
    print("\n" + "-"*80)
    
    # Display resources in alphabetical order
    for index, resource_key in enumerate(sorted_resource_list, 1):
        print(f"  {index:2d}. {resource_key}")
    
    print("\n" + "-"*80)
    
    # Get user selection
    while True:
        selection = input("\nEnter your selection (or press Enter for all): ").strip()
        
        if selection.lower() == 'q':
            print("Exiting...")
            sys.exit(0)
        
        if selection == '' or selection == '0':
            return resource_list  # Return original list to maintain dependency order
        
        try:
            selected_indices = set()
            parts = selection.split(',')
            
            for part in parts:
                part = part.strip()
                if '-' in part:
                    # Range
                    start, end = map(int, part.split('-'))
                    selected_indices.update(range(start, end + 1))
                else:
                    # Single number
                    selected_indices.add(int(part))
            
            # Validate indices
            if any(i < 1 or i > len(sorted_resource_list) for i in selected_indices):
                print(f"Error: Please enter numbers between 1 and {len(sorted_resource_list)}")
                continue
            
            # Convert indices to resource keys using the sorted list
            selected_resources = [sorted_resource_list[i - 1] for i in sorted(selected_indices)]
            
            print(f"\nSelected {len(selected_resources)} resource types")
            return selected_resources
            
        except ValueError:
            print("Error: Invalid input format. Please try again.")
            continue
# ============================================================================
# IMPORT MODE - MAIN EXECUTION
# ============================================================================

def run_import():
    """Run import mode - fetch from API and generate import files"""
    logger.info("=" * 80)
    logger.info(f"CSE - PagerDuty Terraform Import Generator v{__version__}")
    logger.info("MODE: Import - Fetching from PagerDuty API")
    logger.info("=" * 80)
    
    # Validate API credentials
    if not API_TOKEN or API_TOKEN == "YOUR_PAGERDUTY_API_TOKEN_HERE":
        line_num = get_api_token_line_number()
        logger.error("\n❌ ERROR: PagerDuty API token not configured!")
        logger.info("\nPlease configure your API token using one of these methods:")
        logger.info("Option 1 - Environment Variable (Recommended):")
        logger.info(f"  export API_TOKEN='your_pagerduty_token'")
        logger.info(f"  {SCRIPT_NAME} --import\n")
        logger.info("Option 2 - Inline Environment Variable:")
        logger.info(f"  API_TOKEN='your_pagerduty_token' {SCRIPT_NAME} --import\n")
        if line_num:
            logger.info(f"Option 3: Edit line {line_num} in this script to add your_pagerduty_token (NOT Recommended):")
            logger.info(f"  API_TOKEN = os.getenv(\"API_TOKEN\", \"YOUR_PAGERDUTY_API_TOKEN_HERE\")")
        else:
            logger.info("  Option 3: Edit the API_TOKEN line in this script to add your_pagerduty_token")
        return
    
    logger.info(f"Rate limit: {1/RATE_LIMIT_DELAY:.1f} requests/second")
    logger.info(f"Caching: Enabled")
    logger.info("=" * 80)
    
    # Display resource selection menu
    selected_resource_types = display_resource_menu()
    
    # Resolve dependencies
    selected_resource_types = resolve_dependencies(selected_resource_types)
    
    # Filter RESOURCE_CONFIGS to only include selected resources
    selected_configs = {k: v for k, v in RESOURCE_CONFIGS.items() if k in selected_resource_types}
    
    logger.info("\n" + "="*80)
    logger.info(f"Processing {len(selected_configs)} selected resource types")
    logger.info("="*80)

    start_time = time.time()
    stored_resources = {}
    
    # Process each resource type
    for resource_type, config in selected_configs.items():
        logger.info(f"\n[{resource_type}]")
        logger.info("-" * 80)
        
        resources = []
        
        if config.requires_sub_fetch:
            sub_fetch_func = globals()[config.sub_fetch_function]
            
            # Determine parent resources
            if "business_service" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_business_service", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "jira_cloud" in config.sub_fetch_function:
                resources = sub_fetch_func()
            elif "service_integration" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_service", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "team_membership" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_team", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "action_service_association" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_automation_actions_action", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "action_team_association" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_automation_actions_action", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "runner_team_association" in config.sub_fetch_function:
                resources = sub_fetch_func([])
            elif "service_event_rule" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_service", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "tag_assignment" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_tag", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "event_orchestration_integration" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_event_orchestration", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "enablement" in config.sub_fetch_function:
                resources = sub_fetch_func()
            elif "global_cache_variable" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_event_orchestration", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "service_cache_variable" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_service", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "incident_type_custom_field" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_incident_type", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "handoff_notification_rule" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_user", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "service_custom_field_value" in config.sub_fetch_function:
                parent_resources = stored_resources.get("pagerduty_service", [])
                if parent_resources:
                    resources = sub_fetch_func(parent_resources)
            elif "incident_workflow_trigger" in config.sub_fetch_function:
                resources = sub_fetch_func()
            if not resources:
                logger.warning(f"  No resources found for {resource_type}")
        
        elif config.requires_expansion:
            parent_resources = fetch_paginated_resources(config.api_endpoint, config.resource_key)
            
            for parent in parent_resources:
                parent_id = parent.get("id")
                nested_items = parent.get(config.expansion_field, [])
                
                for item in nested_items:
                    item["_user_id"] = parent_id
                    resources.append(item)
        
        else:
            resources = fetch_paginated_resources(config.api_endpoint, config.resource_key)
            
            if config.filter_function:
                original_count = len(resources)
                resources = [r for r in resources if config.filter_function(r)]
                logger.info(f"  Filtered: {original_count} -> {len(resources)} resources")
            
            stored_resources[resource_type] = resources
        
        if resources:
            generate_import_file_for_resource(resource_type, config, resources)
            stored_resources[resource_type] = resources
            logger.info(f"  ✓ Found {len(resources)} resources")
        else:
            logger.info(f"  No resources found for {resource_type}")
    
    # Print statistics
    elapsed_time = time.time() - start_time
    
    # Calculate resource statistics
    total_resources = sum(len(resources) for resources in stored_resources.values())
    resource_types_with_data = len([r for r in stored_resources.values() if len(r) > 0])
    
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total resources found: {total_resources} (owner user will not be processed)")
    logger.info(f"Resource types with data: {resource_types_with_data}/{len(selected_configs)}")
    logger.info(f"Total execution time: {elapsed_time:.2f} seconds")
    logger.info(f"API calls made: {api_cache.misses}")
    logger.info(f"Cached responses reused: {api_cache.hits}")
    if api_cache.hits > 0:
        logger.info(f"API requests saved: {api_cache.hit_rate:.1f}%")
    logger.info("=" * 80)
    logger.info("\nIf successful, next steps:")
    logger.info("1. Review the generated to_import_*.tf files")
    logger.info(f"2. Run: {SCRIPT_NAME} --execute_plan")
    logger.info(f"3. Run: {SCRIPT_NAME} --replace_refs (optional)")
    logger.info(f"4. Run: {SCRIPT_NAME} --structure (optional)")
    logger.info("5. Run: terraform plan (to verify changes)")
    logger.info("6. Run: terraform apply (to apply changes)")
    logger.info(f"7. Run: {SCRIPT_NAME} --cleanup (recommended)")
    logger.info("=" * 80)

# ============================================================================
# EXECUTE PLAN MODE
# ============================================================================

def parse_import_file(filename: str) -> List[str]:
    """Parse a to_import_*.tf file and extract target resources"""
    targets = []
    
    try:
        with open(filename, 'r') as f:
            content = f.read()
        
        # This regex ensures we don't match lines starting with #
        import_blocks = re.findall(r'^import\s*\{[^}]*to\s*=\s*([^\s}]+)', content, re.MULTILINE)
        targets.extend(import_blocks)
        
    except FileNotFoundError:
        logger.warning(f"  File not found: {filename}")
    except Exception as e:
        logger.error(f"  Error parsing {filename}: {e}")
    
    return targets

def run_execute_plan():
    """Run execute plan mode - execute terraform plan commands"""
    logger.info("=" * 80)
    logger.info(f"CSE - PagerDuty Terraform Import Generator v{__version__}")
    logger.info("MODE: Execute Plan - Running terraform plan commands")
    logger.info("=" * 80)
    
    import_files = glob.glob("to_import_*.tf")
    
    if not import_files:
        logger.error("\n❌ ERROR: No to_import_*.tf files found!")
        logger.info(f"Please run the import mode first: {SCRIPT_NAME} --import")
        return
    
    logger.info(f"\nFound {len(import_files)} import files")
    logger.info("=" * 80)
    
    # Track statistics
    total_resources = 0
    successful_files = 0
    failed_files = 0
    
    for import_file in sorted(import_files):
        output_file = import_file.replace("to_import_", "").replace(".tf", "")
        
        logger.info(f"\n[Processing: {import_file}]")
        logger.info("-" * 80)
        
        targets = parse_import_file(import_file)
        
        if not targets:
            logger.info(f"  No targets found in {import_file}, skipping...")
            continue
        
        logger.info(f"  Found {len(targets)} resources to import")
        total_resources += len(targets)
        
        output_filename = f"imported_{output_file}.tf"
        cmd = ["terraform", "plan", f"-generate-config-out={output_filename}"]
        
        for target in targets:
            cmd.extend(["-target", target])
        
        logger.info(f"  Running: {' '.join(cmd[:4])}... (with {len(targets)} targets)")
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info(f"  ✓ Successfully generated {output_filename}")
                successful_files += 1
            else:
                logger.error(f"  ✗ Error running terraform plan:")
                logger.error(f"    {result.stderr}")
                failed_files += 1
                
        except subprocess.TimeoutExpired:
            logger.error(f"  ✗ Timeout: Command took longer than 5 minutes")
            failed_files += 1
        except FileNotFoundError:
            logger.error(f"  ✗ Error: terraform command not found. Is Terraform installed?")
            return
        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            failed_files += 1
    
    # Count generated files
    generated_files = len(glob.glob("imported_*.tf"))
    
    logger.info("\n" + "=" * 80)
    logger.info("EXECUTION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Files processed: {len(import_files)}")
    logger.info(f"Files generated: {generated_files}")
    logger.info(f"Total resources processed: {total_resources} (owner user was not processed)")
    logger.info(f"Successful: {successful_files}")
    if failed_files > 0:
        logger.info(f"⚠️  Failed: {failed_files}")
        logger.info("   (Check logs above for details)")
    logger.info("=" * 80)
    logger.info("\nIf successful, next steps:")
    logger.info("1. Review the generated imported_*.tf files")
    logger.info(f"2. Run: {SCRIPT_NAME} --replace_refs (optional)")
    logger.info(f"3. Run: {SCRIPT_NAME} --structure (optional)")
    logger.info("4. Run: terraform plan (to verify changes)")
    logger.info("5. Run: terraform apply (to apply changes)")
    logger.info(f"6. Run: {SCRIPT_NAME} --cleanup (recommended)")
    logger.info("=" * 80)

# ============================================================================
# REPLACE REFERENCES MODE
# ============================================================================

def build_resource_id_map(imported_files: List[str]) -> Dict[str, Dict[str, str]]:
    """Build a map of resource IDs to their Terraform references"""
    resource_map = {}
    
    tracked_types = [
        'pagerduty_user', 'pagerduty_team', 'pagerduty_service',
        'pagerduty_escalation_policy', 'pagerduty_schedule',
        'pagerduty_business_service', 'pagerduty_event_orchestration',
        'pagerduty_automation_actions_runner',
        'pagerduty_automation_actions_action',
        'pagerduty_incident_workflow',
        'pagerduty_tag',
    ]
    
    for filename in imported_files:
        logger.info(f"  Scanning {filename}...")
        try:
            with open(filename, 'r') as f:
                content = f.read()
            
            # Match: # __generated__ by Terraform from "ID"
            # followed by: resource "type" "name" {
            pattern = r'# __generated__ by Terraform from "([^"]+)"\s*\n\s*resource\s+"([^"]+)"\s+"([^"]+)"\s*\{'
            matches = re.finditer(pattern, content)
            
            for match in matches:
                resource_id = match.group(1)
                resource_type = match.group(2)
                resource_name = match.group(3)
                
                if resource_type not in tracked_types:
                    continue
                
                if resource_type not in resource_map:
                    resource_map[resource_type] = {}
                
                resource_map[resource_type][resource_id] = f"{resource_type}.{resource_name}.id"
        
        except Exception as e:
            logger.error(f"  Error processing {filename}: {e}")
    
    total_tracked = sum(len(ids) for ids in resource_map.values())
    logger.info(f"\n  Found {total_tracked} resources across {len(resource_map)} resource types")
    
    return resource_map

def replace_references_in_file(filename: str, resource_map: Dict[str, Dict[str, str]]) -> Tuple[int, bool]:
    """Replace hardcoded IDs with Terraform references in a file"""
    try:
        with open(filename, 'r') as f:
            content = f.read()
        
        original_content = content
        replacements = 0
        
        for resource_type, id_map in resource_map.items():
            for resource_id, terraform_ref in id_map.items():
                # Escape the resource_id for use in regex
                escaped_id = re.escape(resource_id)
                
                # Pattern 1: Single value assignment: attribute = "ID"
                # Matches: runner_id = "01E8D6A3MRVMHXD5KQNXO3S62D"
                pattern1 = rf'(\s+\w+\s*=\s*)"({escaped_id})"'
                replacement1 = rf'\1{terraform_ref}'
                new_content, count = re.subn(pattern1, replacement1, content)
                if count > 0:
                    replacements += count
                    content = new_content
                
                # Pattern 2: ID in array (handles all positions)
                # Matches: "ID" anywhere in an array context
                pattern2 = rf'"({escaped_id})"'
                # Check if this ID appears in an array context (between [ ])
                # We'll do a more careful replacement
                lines = content.split('\n')
                new_lines = []
                for line in lines:
                    # Check if line contains the ID in quotes and is likely in an array
                    if f'"{resource_id}"' in line and ('[' in line or ',' in line or ']' in line):
                        # Replace the quoted ID with the terraform reference
                        line = line.replace(f'"{resource_id}"', terraform_ref)
                        replacements += 1
                    new_lines.append(line)
                content = '\n'.join(new_lines)
        
        if content != original_content:
            # Create backup
            backup_file = f"{filename}.backup"
            with open(backup_file, 'w') as f:
                f.write(original_content)
            logger.info(f"    📦 Created backup: {backup_file}")
            
            # Write updated content
            with open(filename, 'w') as f:
                f.write(content)
            
            return replacements, True
        
        return 0, False
    
    except Exception as e:
        logger.error(f"  Error processing {filename}: {e}")
        return 0, False

def run_replace_refs():
    """Run replace references mode - replace hardcoded IDs with Terraform references"""
    logger.info("=" * 80)
    logger.info(f"CSE - PagerDuty Terraform Import Generator v{__version__}")
    logger.info("MODE: Replace References - Converting IDs to Terraform references")
    logger.info("=" * 80)
    
    imported_files = glob.glob("imported_*.tf")
    
    if not imported_files:
        logger.error("\n❌ ERROR: No imported_*.tf files found!")
        logger.info(f"Please run the execute plan mode first: {SCRIPT_NAME} -e")
        return
    
    logger.info(f"\nFound {len(imported_files)} imported files")
    logger.info("=" * 80)
    
    logger.info("\n[Step 1: Building resource ID map]")
    logger.info("-" * 80)
    resource_map = build_resource_id_map(imported_files)
    
    logger.info("\n[Step 2: Replacing IDs with Terraform references]")
    logger.info("-" * 80)
    
    total_replacements = 0
    
    for filename in sorted(imported_files):
        logger.info(f"\n  Processing {filename}...")
        replacements, modified = replace_references_in_file(filename, resource_map)
        
        if modified:
            logger.info(f"    ✓ Updated {filename} with {replacements} replacements")
            total_replacements += replacements
        else:
            logger.info(f"    ℹ No replacements needed")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"Total replacements made: {total_replacements}")
    logger.info("=" * 80)
    logger.info("\nIf successful, next steps:")
    logger.info("1. Review the modified imported_*.tf files")
    logger.info(f"2. Run: {SCRIPT_NAME} --structure (optional)")
    logger.info("3. Run: terraform plan (to verify changes)")
    logger.info("4. Run: terraform apply (to apply changes)")
    logger.info(f"5. Run: {SCRIPT_NAME} --cleanup (recommended)")
    logger.info("=" * 80)

# ============================================================================
# CLEANUP MODE
# ============================================================================

def run_cleanup():
    """Remove backup files and import files after successful import"""
    logger.info("=" * 80)
    logger.info(f"CSE - PagerDuty Terraform Import Generator v{__version__}")
    logger.info("MODE: Cleanup - Removing *backup, and to_import_* files")
    logger.info("=" * 80)
    
    backup_files = glob.glob("imported_*.tf.backup")
    import_files = glob.glob("to_import_*.tf")
    
    all_files = backup_files + import_files
    
    if not all_files:
        logger.info("\nNo backup or to_import files found.")
        return
    
    logger.info(f"\nFound {len(backup_files)} backup files, {len(import_files)} to_import files:")
    
    if backup_files:
        logger.info("\nBackup files:")
        for backup_file in sorted(backup_files):
            logger.info(f"  - {backup_file}")
    
    if import_files:
        logger.info("\nImport files:")
        for import_file in sorted(import_files):
            logger.info(f"  - {import_file}")
    
    logger.info("\n" + "=" * 80)
    
    # Ask for confirmation
    try:
        response = input("\nAre you sure you want to delete these files?, you MUST have run 'terraform apply' first (yes/no): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        logger.info("\n\nOperation cancelled.")
        return
    
    if response not in ['yes', 'y']:
        logger.info("\nOperation cancelled.")
        return
    
    logger.info("\n" + "=" * 80)
    logger.info("Removing files...")
    logger.info("-" * 80)
    
    removed_count = 0
    for file_path in sorted(all_files):
        try:
            os.remove(file_path)
            logger.info(f"  ✓ Removed {file_path}")
            removed_count += 1
        except Exception as e:
            logger.error(f"  ✗ Error removing {file_path}: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"CLEANUP COMPLETE - Removed {removed_count}/{len(all_files)} files")
    logger.info("=" * 80)

# ============================================================================
# STRUCTURE MODE
# ============================================================================

def run_structure():
    """Structure imported files by combining related resources"""
    logger.info("=" * 80)
    logger.info(f"CSE - PagerDuty Terraform Import Generator v{__version__}")
    logger.info("MODE: Structure - Organizing imported files into logical groups")
    logger.info("=" * 80)
    
    imported_files = glob.glob("imported_*.tf")
    
    if not imported_files:
        logger.info("\n❌ ERROR: No imported_*.tf files found!")
        logger.info(f"Please run the execute plan mode first: {SCRIPT_NAME} -e")
        return
    
    logger.info(f"\nFound {len(imported_files)} imported files")
    logger.info("=" * 80)
    
    # Define organization structure
    organization_map = {
        'users.tf': [
            'imported_users.tf',
            'imported_user_contact_methods.tf',
            'imported_user_notification_rules.tf',
            'imported_user_handoff_notification_rules.tf'
        ],
        'teams.tf': [
            'imported_teams.tf',
            'imported_team_memberships.tf'
        ],
        'services.tf': [
            'imported_services.tf',
            'imported_service_integrations.tf',
            'imported_service_event_rules.tf',
            'imported_service_custom_fields.tf',
            'imported_service_custom_field_values.tf'
        ],
        'schedules.tf': [
            'imported_schedules.tf'
        ],
        'escalation_policies.tf': [
            'imported_escalation_policies.tf'
        ],
        'event_orchestrations.tf': [
            'imported_event_orchestrations.tf',
            'imported_event_orchestration_globals.tf',
            'imported_event_orchestration_routers.tf',
            'imported_event_orchestration_services.tf',
            'imported_event_orchestration_unrouteds.tf',
            'imported_event_orchestration_integrations.tf',
            'imported_event_orchestration_global_cache_variables.tf',
            'imported_event_orchestration_service_cache_variables.tf'
        ],
        'incident_workflows.tf': [
            'imported_incident_workflows.tf',
            'imported_incident_workflow_triggers.tf'
        ],
        'incident_types.tf': [
            'imported_incident_types.tf',
            'imported_incident_type_custom_fields.tf',
            'imported_incident_custom_fields.tf'
        ],
        'business_services.tf': [
            'imported_business_services.tf',
            'imported_business_service_subscribers.tf'
        ],
        'automation_actions.tf': [
            'imported_automation_actions_actions.tf',
            'imported_automation_actions_runners.tf',
            'imported_automation_actions_action_service_associations.tf',
            'imported_automation_actions_action_team_associations.tf',
            'imported_automation_actions_runner_team_associations.tf'
        ],
        'tags.tf': [
            'imported_tags.tf',
            'imported_tag_assignments.tf'
        ],
        'extensions.tf': [
            'imported_extensions.tf'
        ],
        'addons.tf': [
            'imported_addons.tf'
        ],
        'alert_grouping.tf': [
            'imported_alert_grouping_settings.tf'
        ],
        'enablements.tf': [
            'imported_enablements.tf'
        ],
        'jira_integration.tf': [
            'imported_jira_cloud_account_mapping_rules.tf'
        ],
        'webhooks.tf': [
            'imported_webhook_subscriptions.tf'
        ]
    }
    
    # Preview what will be merged
    logger.info("\nPREVIEW: Files to be merged")
    logger.info("-" * 80)
    
    preview_count = 0
    files_to_process = set()
    
    for target_file, source_files in organization_map.items():
        existing_sources = [f for f in source_files if os.path.exists(f)]
        if existing_sources:
            logger.info(f"\n  {target_file}")
            for source_file in existing_sources:
                logger.info(f"    ← {source_file}")
                files_to_process.add(source_file)
            preview_count += 1
    
    # Show remaining files
    remaining_files = [f for f in imported_files if f not in files_to_process]
    if remaining_files:
        logger.info(f"\n  Files to be renamed (imported_* → *):")
        for remaining_file in remaining_files:
            new_name = remaining_file.replace('imported_', '')
            logger.info(f"    {remaining_file} → {new_name}")
    
    logger.info("\n" + "-" * 80)
    logger.info(f"Total files to merge: {len(files_to_process)}")
    logger.info(f"Total files to rename: {len(remaining_files)}")
    logger.info(f"New structured files to create: {preview_count}")
    logger.info("-" * 80)
    
    # Ask for confirmation
    try:
        response = input("\nThis will restructure your imported files. Continue? (yes/no): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        logger.info("\n\nOperation cancelled.")
        return
    
    if response not in ['yes', 'y']:
        logger.info("\nOperation cancelled.")
        return
    
    logger.info("\n" + "=" * 80)
    logger.info("Structuring files...")
    logger.info("-" * 80)
    
    processed_files = set()
    files_created = 0
    files_merged = 0
    files_renamed = 0
    
    for target_file, source_files in organization_map.items():
        content_parts = []
        files_combined = []
        
        for source_file in source_files:
            if os.path.exists(source_file):
                try:
                    with open(source_file, 'r') as f:
                        content = f.read()
                    
                    # Separator comment
                    content_parts.append(f"\n# ============================================================================\n")
                    content_parts.append(f"# {source_file.replace('imported_', '').replace('.tf', '').replace('_', ' ').title()}\n")
                    content_parts.append(f"# ============================================================================\n\n")
                    content_parts.append(content)
                    
                    files_combined.append(source_file)
                    processed_files.add(source_file)
                except Exception as e:
                    logger.error(f"  ✗ Error reading {source_file}: {e}")
        
        if content_parts:
            try:
                with open(target_file, 'w') as f:
                    f.write(''.join(content_parts))
                
                logger.info(f"\n  ✓ Created {target_file}")
                logger.info(f"    Merged {len(files_combined)} files")
                
                files_created += 1
                files_merged += len(files_combined)
                
                # Remove source files
                for source_file in files_combined:
                    try:
                        os.remove(source_file)
                    except Exception as e:
                        logger.error(f"    ✗ Error removing {source_file}: {e}")
            except Exception as e:
                logger.error(f"  ✗ Error creating {target_file}: {e}")
    
    # Handle any remaining imported files not in the map
    remaining_files = [f for f in imported_files if f not in processed_files]
    if remaining_files:
        logger.info("\n  Renaming remaining files:")
        for remaining_file in remaining_files:
            new_name = remaining_file.replace('imported_', '')
            try:
                os.rename(remaining_file, new_name)
                logger.info(f"    ✓ {remaining_file} → {new_name}")
                files_renamed += 1
            except Exception as e:
                logger.error(f"    ✗ Error renaming {remaining_file}: {e}")
    
    logger.info("\n" + "=" * 80)
    logger.info("STRUCTURE COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Structured files created: {files_created}")
    logger.info(f"Files merged: {files_merged}")
    if files_renamed > 0:
        logger.info(f"Files renamed: {files_renamed}")
        logger.info(f"Total operations: {files_merged + files_renamed}")
    logger.info("=" * 80)
    logger.info("\nIf successful, next steps:")
    logger.info("1. Review the structured .tf files")
    logger.info("2. Run: terraform plan (to verify, should be no changes)")
    logger.info("3. Run: terraform apply (to apply changes)")
    logger.info(f"4. Run: {SCRIPT_NAME} --cleanup (recommended)")
    logger.info("=" * 80)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description=f"CSE - PagerDuty Terraform Import Generator v{__version__}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch from API and generate import files
  %(prog)s --import
  or
  %(prog)s -i
  
  # Execute terraform plan commands
  %(prog)s --execute_plan
  or
  %(prog)s -e
  
  # Replace hardcoded IDs with Terraform references
  %(prog)s --replace_refs
  or
  %(prog)s -r
  
  # Structure imported files into logical groups
  %(prog)s --structure
  or
  %(prog)s -s
  
  # Cleanup backup and import files
  %(prog)s --cleanup
  or
  %(prog)s -c
  
  # Full workflow
  %(prog)s -i
  %(prog)s -e
  %(prog)s -r #(optional)
  %(prog)s -s #(optional)
  terraform plan
  terraform apply
  %(prog)s -c #(recommended)
        """
    )
    
    parser.add_argument('-i', '--import', dest='import_mode', action='store_true',
                        help='Fetch from API and generate import files')
    parser.add_argument('-e', '--execute_plan', dest='execute_plan', action='store_true',
                        help='Execute terraform plan commands')
    parser.add_argument('-r', '--replace_refs', dest='replace_refs', action='store_true',
                        help='(OPTIONAL) Replace common hardcoded IDs with Terraform references')
    parser.add_argument('-s', '--structure', dest='structure', action='store_true',
                        help='(OPTIONAL) Structure imported files into logical groups')
    parser.add_argument('-c', '--cleanup', dest='cleanup', action='store_true',
                        help='(RECOMMENDED) Remove "./*backup", "./imported_*", and "./to_import_*" files after a successful "terraform apply"')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    
    args = parser.parse_args()
    
    if args.import_mode:
        run_import()
    elif args.execute_plan:
        run_execute_plan()
    elif args.replace_refs:
        run_replace_refs()
    elif args.structure:
        run_structure()
    elif args.cleanup:
        run_cleanup()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()