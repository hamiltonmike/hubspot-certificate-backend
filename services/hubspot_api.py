"""
HubSpot API Service Module
Reusable functions for HubSpot API operations

UPDATED: Nov 17, 2025 - Fixed hs_timestamp to use Unix milliseconds (v6.10)
"""

import requests
from datetime import datetime
import time

def get_headers(access_token):
    """Get standard HubSpot API headers"""
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

def search_company_by_name(access_token, company_name):
    """
    Search for company by exact name match
    
    Args:
        access_token: HubSpot API access token
        company_name: Company name to search for
        
    Returns:
        Company object if found, None otherwise
    """
    try:
        url = "https://api.hubapi.com/crm/v3/objects/companies/search"
        headers = get_headers(access_token)
        
        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "name",
                    "operator": "EQ",
                    "value": company_name
                }]
            }],
            "properties": ["name", "domain", "company_type"],
            "limit": 1
        }
        
        response = requests.post(url, headers=headers, json=search_body, timeout=30)
        
        if response.status_code == 200:
            results = response.json().get('results', [])
            if results:
                print(f"DEBUG Found existing company: {company_name} (ID: {results[0]['id']})")
                return results[0]
        
        print(f"DEBUG Company not found: {company_name}")
        return None
        
    except Exception as e:
        print(f"ERROR searching for company: {str(e)}")
        return None

def create_company(access_token, properties):
    """
    Create a new company in HubSpot
    
    Args:
        access_token: HubSpot API access token
        properties: Dictionary of company properties
        
    Returns:
        Company object if created successfully, None otherwise
    """
    try:
        url = "https://api.hubapi.com/crm/v3/objects/companies"
        headers = get_headers(access_token)
        
        data = {"properties": properties}
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code in [200, 201]:
            company = response.json()
            print(f"DEBUG Created company: {properties.get('name')} (ID: {company['id']})")
            return company
        else:
            print(f"ERROR creating company: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"ERROR creating company: {str(e)}")
        return None

def update_company(access_token, company_id, properties):
    """
    Update company properties in HubSpot
    
    Args:
        access_token: HubSpot API access token
        company_id: ID of company to update
        properties: Dictionary of properties to update
        
    Returns:
        True if successful, False otherwise
    """
    try:
        url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        headers = get_headers(access_token)
        
        data = {"properties": properties}
        
        response = requests.patch(url, headers=headers, json=data, timeout=30)
        
        if response.status_code == 200:
            print(f"DEBUG Updated company {company_id}")
            return True
        else:
            print(f"ERROR updating company: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"ERROR updating company: {str(e)}")
        return False

def create_contact(access_token, properties):
    """
    Create a new contact in HubSpot
    
    Args:
        access_token: HubSpot API access token
        properties: Dictionary of contact properties
        
    Returns:
        Contact object if created successfully, None otherwise
    """
    try:
        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        headers = get_headers(access_token)
        
        data = {"properties": properties}
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code in [200, 201]:
            contact = response.json()
            print(f"DEBUG Created contact: {properties.get('email')} (ID: {contact['id']})")
            return contact
        else:
            print(f"ERROR creating contact: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"ERROR creating contact: {str(e)}")
        return None

def associate_records(access_token, from_type, from_id, to_type, to_id, custom_type_id=None):
    """
    Create association between two HubSpot records
    
    Args:
        access_token: HubSpot API access token
        from_type: Object type of source record (e.g., 'contact', 'company')
        from_id: ID of source record
        to_type: Object type of target record
        to_id: ID of target record
        custom_type_id: Optional custom association type ID (for labeled associations)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Map of default association type IDs for different object combinations
        # Based on HubSpot v4 API documentation
        association_type_map = {
            ('contact', 'company'): 1,
            ('contact', 'ticket'): 15,
            ('contact', 'deal'): 4,
            ('company', 'contact'): 2,
            ('company', 'ticket'): 25,
            ('company', 'deal'): 6,
            ('deal', 'contact'): 3,
            ('deal', 'company'): 5,
            ('deal', 'ticket'): 27,
            ('ticket', 'contact'): 16,
            ('ticket', 'company'): 26,
            ('ticket', 'deal'): 28,
        }
        
        # Use custom type ID if provided, otherwise use default mapping
        if custom_type_id:
            association_type_id = custom_type_id
            association_category = "USER_DEFINED"
        else:
            type_key = (from_type, to_type)
            association_type_id = association_type_map.get(type_key, 1)
            association_category = "HUBSPOT_DEFINED"
        
        url = f"https://api.hubapi.com/crm/v4/objects/{from_type}/{from_id}/associations/{to_type}/{to_id}"
        headers = get_headers(access_token)
        
        association_spec = [{"associationCategory": association_category, "associationTypeId": association_type_id}]
        response = requests.put(url, headers=headers, json=association_spec, timeout=30)
        
        if response.status_code in [200, 201]:
            print(f"DEBUG Created association: {from_type}:{from_id} → {to_type}:{to_id} (typeId: {association_type_id})")
            return True
        else:
            print(f"ERROR Association failed: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"ERROR creating association: {str(e)}")
        return False

def create_note_on_ticket(access_token, ticket_id, note_body):
    """
    Create a note on a ticket using v3 Objects API with associations
    
    Args:
        access_token: HubSpot API access token
        ticket_id: ID of ticket to add note to
        note_body: HTML content of the note
        
    Returns:
        True if successful, False otherwise
        
    Uses association type ID 228 for note→ticket per HubSpot v3 API spec
    """
    try:
        url = "https://api.hubapi.com/crm/v3/objects/notes"
        headers = get_headers(access_token)
        
        data = {
            "properties": {
                "hs_timestamp": str(int(time.time() * 1000)),
                "hs_note_body": note_body
            },
            "associations": [
                {
                    "to": {
                        "id": str(ticket_id)
                    },
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 228
                        }
                    ]
                }
            ]
        }
        
        response = requests.post(url, headers=headers, json=data, timeout=30)
        
        if response.status_code in [200, 201]:
            note = response.json()
            print(f"DEBUG Created note on ticket {ticket_id}: {note['id']}")
            return True
        else:
            print(f"ERROR creating note: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"ERROR creating note: {str(e)}")
        return False


def get_company_property(access_token, company_id, property_name):
    """
    Get a specific property value from a company
    
    Args:
        access_token: HubSpot API access token
        company_id: ID of company
        property_name: Name of property to retrieve
        
    Returns:
        Property value if found, None otherwise
    """
    try:
        url = f"https://api.hubapi.com/crm/v3/objects/companies/{company_id}"
        headers = get_headers(access_token)
        params = {"properties": property_name}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            company = response.json()
            value = company.get('properties', {}).get(property_name)
            print(f"DEBUG Got property {property_name} from company {company_id}: {value}")
            return value
        else:
            print(f"ERROR getting company property: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"ERROR getting company property: {str(e)}")
        return None

def extract_domain_from_email(email):
    """
    Extract domain from email address
    
    Args:
        email: Email address
        
    Returns:
        Domain (e.g., 'example.com')
    """
    if not email or '@' not in email:
        return None
    return email.split('@')[1].lower()

def check_domain_match(email, company_domain):
    """
    Check if email domain matches company domain
    
    Args:
        email: Contact's email address
        company_domain: Company's domain
        
    Returns:
        True if domains match (or company_domain is None), False otherwise
    """
    if not company_domain:
        # No company domain to check against - pass
        return True
    
    email_domain = extract_domain_from_email(email)
    if not email_domain:
        return False
    
    # Normalize domains for comparison
    company_domain = company_domain.lower().replace('www.', '')
    email_domain = email_domain.lower().replace('www.', '')
    
    return email_domain == company_domain
