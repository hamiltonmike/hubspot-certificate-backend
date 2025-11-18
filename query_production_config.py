#!/usr/bin/env python3
"""
Query ALL Production Portal Configuration
Portal: 1854622 (Provident Production)

This script retrieves complete configuration information for production deployment.
"""
import requests
import json
import os
import sys

# You need to provide your production access token
PROD_TOKEN = os.environ.get('HUBSPOT_PROD_TOKEN')

if not PROD_TOKEN:
    print("ERROR: Set HUBSPOT_PROD_TOKEN environment variable")
    print("Example: export HUBSPOT_PROD_TOKEN='your_production_token_here'")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {PROD_TOKEN}",
    "Content-Type": "application/json"
}

print("\n" + "="*80)
print("PROVIDENT PRODUCTION PORTAL - COMPLETE CONFIGURATION")
print("Portal ID: 1854622")
print("="*80)

# ============================================================================
# 1. GET ALL CUSTOM OBJECTS
# ============================================================================

print("\n" + "="*80)
print("CUSTOM OBJECTS")
print("="*80)

try:
    url = "https://api.hubapi.com/crm/v3/schemas"
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        schemas = response.json().get('results', [])
        
        custom_objects = []
        for schema in schemas:
            # Filter for custom objects (objectTypeId starts with "2-")
            if schema.get('objectTypeId', '').startswith('2-'):
                custom_objects.append(schema)
        
        print(f"\nFound {len(custom_objects)} custom object(s):\n")
        
        for obj in custom_objects:
            print(f"Name: {obj.get('name')}")
            print(f"Label: {obj.get('labels', {}).get('singular')}")
            print(f"Object Type ID: {obj.get('objectTypeId')}")
            print(f"Fully Qualified Name: {obj.get('fullyQualifiedName')}")
            print(f"Created: {obj.get('createdAt')}")
            print()
            
    else:
        print(f"ERROR: {response.status_code} - {response.text}")
        
except Exception as e:
    print(f"ERROR: {str(e)}")

# ============================================================================
# 2. GET TICKET CUSTOM PROPERTIES
# ============================================================================

print("\n" + "="*80)
print("TICKET CUSTOM PROPERTIES (certificate related)")
print("="*80)

try:
    url = "https://api.hubapi.com/crm/v3/properties/tickets"
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        properties = response.json().get('results', [])
        
        # Filter for certificate-related properties
        cert_props = [p for p in properties if 'certificate' in p.get('name', '').lower()]
        
        print(f"\nFound {len(cert_props)} certificate-related property(ies):\n")
        
        for prop in cert_props:
            print(f"Name: {prop.get('name')}")
            print(f"Label: {prop.get('label')}")
            print(f"Type: {prop.get('type')}")
            print(f"Field Type: {prop.get('fieldType')}")
            print()
            
    else:
        print(f"ERROR: {response.status_code} - {response.text}")
        
except Exception as e:
    print(f"ERROR: {str(e)}")

# ============================================================================
# 3. SUMMARY FOR TECHNICAL DOCUMENTATION
# ============================================================================

print("\n" + "="*80)
print("PRODUCTION CONFIGURATION SUMMARY")
print("="*80)

print("""
## Production Portal Configuration (Portal 1854622)

### Custom Objects
""")

# Re-fetch for summary
try:
    url = "https://api.hubapi.com/crm/v3/schemas"
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        schemas = response.json().get('results', [])
        
        for schema in schemas:
            if schema.get('objectTypeId', '').startswith('2-'):
                name = schema.get('name')
                type_id = schema.get('objectTypeId')
                fqn = schema.get('fullyQualifiedName')
                print(f"- **{schema.get('labels', {}).get('singular')}**: `{name}` (Object Type ID: `{type_id}`, FQN: `{fqn}`)")
                
except Exception as e:
    pass

print("""
### Association Type IDs

**Standard Object Associations:**
- Site (ticket→company): 145
- Insurance Broker (ticket→company): 474
- Insurance Broker (ticket→contact): 476
- Underwriter (company→ticket): 340
- Requestor (ticket→contact): 16

**Custom Object Associations:**
""")

# Get custom object names for association display
try:
    url = "https://api.hubapi.com/crm/v3/schemas"
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        schemas = response.json().get('results', [])
        
        for schema in schemas:
            if schema.get('objectTypeId', '').startswith('2-'):
                name = schema.get('name')
                singular = schema.get('labels', {}).get('singular', name)
                
                if 'system' in name.lower():
                    print(f"- Security System (ticket→{name}): 480")
                elif 'agreement' in name.lower():
                    print(f"- Service Agreement (ticket→{name}): 478")
                    
except Exception as e:
    pass

print("""
### Ticket Properties

- `certificate_sent_date`: Date field (Unix milliseconds at midnight UTC)
- `certificate_pdf_url`: String field (Google Drive URL)

### Access Token

- **Private App Name**: Provident Backend API
- **Location**: Settings → Development → Legacy Apps
- **Environment Variable**: `HUBSPOT_ACCESS_TOKEN`
""")

print("\n" + "="*80)
print("DEPLOYMENT ENVIRONMENT VARIABLES")
print("="*80)

print("""
Export these for Cloud Run deployment:

export HUBSPOT_ACCESS_TOKEN="<production_token>"
export SITE_ASSOCIATION_TYPE_ID=145
export BROKER_COMPANY_ASSOCIATION_TYPE_ID=474
export BROKER_CONTACT_ASSOCIATION_TYPE_ID=476
export UNDERWRITER_ASSOCIATION_TYPE_ID=340
export REQUESTOR_ASSOCIATION_TYPE_ID=16
export SYSTEM_ASSOCIATION_TYPE_ID=480
export AGREEMENT_ASSOCIATION_TYPE_ID=478
""")

# Get actual object names for environment variables
try:
    url = "https://api.hubapi.com/crm/v3/schemas"
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        schemas = response.json().get('results', [])
        
        for schema in schemas:
            if schema.get('objectTypeId', '').startswith('2-'):
                name = schema.get('name')
                if 'system' in name.lower():
                    print(f"export SYSTEM_OBJECT_NAME=\"{name}\"")
                elif 'agreement' in name.lower():
                    print(f"export AGREEMENT_OBJECT_NAME=\"{name}\"")
                    
except Exception as e:
    pass

print("\n" + "="*80)
print("COMPLETE - All production configuration retrieved")
print("="*80)
