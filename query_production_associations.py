#!/usr/bin/env python3
"""
Query Production Portal Association Type IDs
Portal: 1854622 (Provident Production)

This script will show you the EXACT association type IDs needed for production.
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

def query_associations(from_obj, to_obj, description):
    """Query association types between two objects"""
    print(f"\n{'='*70}")
    print(f"{description}")
    print(f"Query: {from_obj} → {to_obj}")
    print(f"{'='*70}")
    
    url = f"https://api.hubapi.com/crm/v4/associations/{from_obj}/{to_obj}/labels"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                print(f"⚠️  No associations found")
                return None
            
            print(f"\nFound {len(results)} association type(s):\n")
            
            for assoc in results:
                label = assoc.get('label') or '(unlabeled/default)'
                type_id = assoc.get('typeId')
                category = assoc.get('category', 'HUBSPOT_DEFINED')
                
                print(f"  Label: {label}")
                print(f"  Type ID: {type_id}")
                print(f"  Category: {category}")
                print()
            
            return results
        else:
            print(f"❌ Error {response.status_code}: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Exception: {str(e)}")
        return None

# ============================================================================
# QUERY ALL REQUIRED ASSOCIATIONS
# ============================================================================

print("\n" + "="*70)
print("PROVIDENT PRODUCTION PORTAL ASSOCIATION TYPE IDs")
print("Portal ID: 1854622")
print("="*70)

# Store results
production_ids = {}

# 1. Ticket → Company (for Site label)
results = query_associations('ticket', 'company', '1. SITE ASSOCIATION (Ticket → Company)')
if results:
    for assoc in results:
        if assoc.get('label') == 'Site':
            production_ids['site_type_id'] = assoc['typeId']
            print(f"✅ Found 'Site' label: {assoc['typeId']}")

# 2. Ticket → Company (for Insurance Broker label)  
if results:  # Same query, different label
    for assoc in results:
        if assoc.get('label') == 'Insurance Broker':
            production_ids['broker_company_type_id'] = assoc['typeId']
            print(f"✅ Found 'Insurance Broker' (company) label: {assoc['typeId']}")

# 3. Company → Ticket (for underwriter - default unlabeled)
results = query_associations('company', 'ticket', '2. UNDERWRITER ASSOCIATION (Company → Ticket)')
if results:
    for assoc in results:
        if assoc.get('label') is None:
            production_ids['underwriter_type_id'] = assoc['typeId']
            print(f"✅ Found default unlabeled: {assoc['typeId']}")

# 4. Ticket → Contact (for Insurance Broker label)
results = query_associations('ticket', 'contact', '3. INSURANCE BROKER CONTACT (Ticket → Contact)')
if results:
    for assoc in results:
        if assoc.get('label') == 'Insurance Broker':
            production_ids['broker_contact_type_id'] = assoc['typeId']
            print(f"✅ Found 'Insurance Broker' (contact) label: {assoc['typeId']}")

# 5. Ticket → Contact (for requestor - default)
if results:  # Same query, different label
    for assoc in results:
        if assoc.get('label') is None:
            production_ids['requestor_type_id'] = assoc['typeId']
            print(f"✅ Found default requestor: {assoc['typeId']}")

# 6. Ticket → p_system
results = query_associations('ticket', 'p_system', '4. SYSTEM ASSOCIATION (Ticket → p_system)')
if results:
    for assoc in results:
        if assoc.get('label') is None:
            production_ids['system_type_id'] = assoc['typeId']
            print(f"✅ Found default System association: {assoc['typeId']}")

# 7. Ticket → p_agreement
results = query_associations('ticket', 'p_agreement', '5. AGREEMENT ASSOCIATION (Ticket → p_agreement)')
if results:
    for assoc in results:
        if assoc.get('label') is None:
            production_ids['agreement_type_id'] = assoc['typeId']
            print(f"✅ Found default Agreement association: {assoc['typeId']}")

# ============================================================================
# SUMMARY & NEXT STEPS
# ============================================================================

print("\n" + "="*70)
print("SUMMARY - PRODUCTION ASSOCIATION TYPE IDs")
print("="*70)

print("\nAdd these to Cloud Run environment variables:\n")

for key, value in production_ids.items():
    env_var = key.upper()
    print(f"  {env_var}={value}")

print("\n" + "="*70)
print("DEPLOYMENT COMMAND")
print("="*70)

env_vars = ",".join([f"{k.upper()}={v}" for k, v in production_ids.items()])

print(f"""
gcloud run services update hubspot-certificate-backend \\
  --region=us-central1 \\
  --update-env-vars HUBSPOT_ACCESS_TOKEN=production_token_here,{env_vars}
""")

print("\n" + "="*70)
print("SANDBOX vs PRODUCTION COMPARISON")
print("="*70)

sandbox_ids = {
    'Site label': 293,
    'Insurance Broker (company)': 379,
    'Insurance Broker (contact)': 377,
    'Underwriter': 25,
    'Requestor': 16
}

print("\n| Association | Sandbox ID | Production ID | Match? |")
print("|-------------|------------|---------------|--------|")

prod_site = production_ids.get('site_type_id', '?')
print(f"| Site | 293 | {prod_site} | {'✅' if prod_site == 293 else '❌'} |")

prod_broker_co = production_ids.get('broker_company_type_id', '?')
print(f"| Broker Company | 379 | {prod_broker_co} | {'✅' if prod_broker_co == 379 else '❌'} |")

prod_broker_ct = production_ids.get('broker_contact_type_id', '?')
print(f"| Broker Contact | 377 | {prod_broker_ct} | {'✅' if prod_broker_ct == 377 else '❌'} |")

prod_underwriter = production_ids.get('underwriter_type_id', '?')
print(f"| Underwriter | 25 | {prod_underwriter} | {'✅' if prod_underwriter == 25 else '❌'} |")

prod_requestor = production_ids.get('requestor_type_id', '?')
print(f"| Requestor | 16 | {prod_requestor} | {'✅' if prod_requestor == 16 else '❌'} |")

prod_system = production_ids.get('system_type_id', '?')
print(f"| System | ? | {prod_system} | - |")

prod_agreement = production_ids.get('agreement_type_id', '?')
print(f"| Agreement | ? | {prod_agreement} | - |")

print()
