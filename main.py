"""
PROVIDENT SECURITY - CERTIFICATE GENERATOR BACKEND
Flask API service for generating and sending security monitoring certificates

REVISION HISTORY:
- 2025-11-17 v6.13: CORRECTED - Portal-agnostic + Requestor Filtering
  - FIXED: Restored all environment variables (HUBSPOT_PORTAL_ID, SYSTEM_OBJECT_TYPE_ID, AGREEMENT_OBJECT_TYPE_ID, GCS_BUCKET_NAME)
  - FIXED: Portal ID in note links now uses HUBSPOT_PORTAL_ID env var instead of hardcoded 49576985
  - FIXED: PDF upload security restored to PUBLIC_NOT_INDEXABLE (was PUBLIC_INDEXABLE)
  - CONFIRMED: AUTHORIZED_SITE_ADMIN_IDS = [263, 280] correct for production
  - NEW: Requestor endpoint filters strictly:
    - Only contacts with association type 263 (🦉 SITE ADMIN) or 280 (🦉🦉 SITE SUPER ADMIN)
    - OR contacts associated to Agreement as "Signer" (type 395)
    - Returns success: False with clear error if none qualify

- 2025-11-17 v6.12: UX IMPROVEMENTS - Better error messages
  - ADDED: Error messages when no systems found (instead of empty array)
  - ADDED: Error messages when no agreements found (instead of empty array)
  - ADDED: Error messages when no requestors found (instead of empty array)
  - ADDED: Error messages when no broker contacts found (instead of empty array)

- 2025-11-17 v6.11: PRODUCTION PORTAL SUPPORT - Environment-based configuration
  - CRITICAL: Association type IDs are portal-specific, loaded from environment variables
  - CRITICAL: Custom object names differ (production: system/agreement, sandbox: p_system/p_agreement)
  - VERIFIED: Full HubSpot Platform 2025.2 compliance with v4 Associations API

DEPLOYMENT:
- Platform: Google Cloud Run
- Region: us-central1
- URL: https://hubspot-certificate-backend-486092186709.us-central1.run.app
"""

import os
import hmac
import hashlib
import base64
import json
import requests
import tempfile
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import storage
from datetime import timedelta, datetime, timezone
import uuid
import traceback
from pdf2image import convert_from_path
from PIL import Image
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Import our service modules
from services.google_drive import (
    get_or_create_folder,
    upload_file_to_folder,
    create_shortcut
)
from services.hubspot_api import (
    get_headers,
    search_company_by_name,
    create_company,
    update_company,
    create_contact,
    associate_records,
    create_note_on_ticket,
    get_company_property,
    check_domain_match,
    extract_domain_from_email
)

app = Flask(__name__)

# Configure CORS
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# ============================================================================
# ENVIRONMENT VARIABLES - PORTAL AGNOSTIC
# ============================================================================

def get_required_env(var_name, description=""):
    """Get required environment variable or raise error with helpful message"""
    value = os.environ.get(var_name)
    if not value:
        error_msg = f"Missing required environment variable: {var_name}"
        if description:
            error_msg += f" ({description})"
        raise ValueError(error_msg)
    return value

CLIENT_SECRET = os.environ.get('CLIENT_SECRET', '')
WEBMERGE_URL = os.environ.get(
    'WEBMERGE_URL',
    'https://www.webmerge.me/merge/1238246/45hyg1?download=1'
)
HUBSPOT_ACCESS_TOKEN = os.environ.get('HUBSPOT_ACCESS_TOKEN', '')

# Portal-specific configuration (allows easy sandbox/prod switching)
HUBSPOT_PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "1854622")
SYSTEM_TYPE_ID = os.environ.get("SYSTEM_OBJECT_TYPE_ID", "2-2532422")
AGREEMENT_TYPE_ID = os.environ.get("AGREEMENT_OBJECT_TYPE_ID", "2-16284422")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "provident-certificates-temp")

# ============================================================================
# ASSOCIATION TYPE IDs - PORTAL SPECIFIC
# ============================================================================

# Standard object associations
SITE_ASSOCIATION_TYPE_ID = int(get_required_env('SITE_ASSOCIATION_TYPE_ID', 'ticket→company with Site label'))
BROKER_COMPANY_ASSOCIATION_TYPE_ID = int(get_required_env('BROKER_COMPANY_ASSOCIATION_TYPE_ID', 'ticket→company with Insurance Broker label'))
BROKER_CONTACT_ASSOCIATION_TYPE_ID = int(get_required_env('BROKER_CONTACT_ASSOCIATION_TYPE_ID', 'ticket→contact with Insurance Broker label'))
UNDERWRITER_ASSOCIATION_TYPE_ID = int(get_required_env('UNDERWRITER_ASSOCIATION_TYPE_ID', 'company→ticket default'))
REQUESTOR_ASSOCIATION_TYPE_ID = int(get_required_env('REQUESTOR_ASSOCIATION_TYPE_ID', 'ticket→contact default'))

# Custom object associations with labels
SYSTEM_ASSOCIATION_TYPE_ID = int(get_required_env('SYSTEM_ASSOCIATION_TYPE_ID', 'ticket→system with Security System label'))
AGREEMENT_ASSOCIATION_TYPE_ID = int(get_required_env('AGREEMENT_ASSOCIATION_TYPE_ID', 'ticket→agreement with Service Agreement label'))

# Custom object names (differ between portals)
SYSTEM_OBJECT_NAME = os.environ.get('SYSTEM_OBJECT_NAME', 'system')
AGREEMENT_OBJECT_NAME = os.environ.get('AGREEMENT_OBJECT_NAME', 'agreement')

print("=" * 80, flush=True)
print("CERTIFICATE GENERATOR v6.13 CORRECTED - STARTING", flush=True)
print("=" * 80, flush=True)
print("Portal-specific configuration loaded:", flush=True)
print(f"  Portal ID: {HUBSPOT_PORTAL_ID}", flush=True)
print(f"  Custom Objects: {SYSTEM_OBJECT_NAME}, {AGREEMENT_OBJECT_NAME}", flush=True)
print("  Object Type IDs:", flush=True)
print(f"    System type: {SYSTEM_TYPE_ID}", flush=True)
print(f"    Agreement type: {AGREEMENT_TYPE_ID}", flush=True)
print("  Association IDs:", flush=True)
print(f"    Site: {SITE_ASSOCIATION_TYPE_ID}", flush=True)
print(f"    Broker Company: {BROKER_COMPANY_ASSOCIATION_TYPE_ID}", flush=True)
print(f"    Broker Contact: {BROKER_CONTACT_ASSOCIATION_TYPE_ID}", flush=True)
print(f"    System: {SYSTEM_ASSOCIATION_TYPE_ID}", flush=True)
print(f"    Agreement: {AGREEMENT_ASSOCIATION_TYPE_ID}", flush=True)
print(f"    Underwriter: {UNDERWRITER_ASSOCIATION_TYPE_ID}", flush=True)
print(f"    Requestor: {REQUESTOR_ASSOCIATION_TYPE_ID}", flush=True)
print(f"  GCS Bucket: {GCS_BUCKET_NAME}", flush=True)
print("=" * 80, flush=True)

# Email Configuration
CERTIFICATE_EMAIL_TEMPLATE_ID = 199695511179
SENDER_EMAIL = "customerservice@providentsecurity.ca"

# TESTING MODE
TESTING_MODE = True
TEST_EMAIL_OVERRIDE = "mike+testing@providentsecurity.ca"

# Association Type IDs for requestors (PRODUCTION VALUES)
AUTHORIZED_SITE_ADMIN_IDS = [263, 280]  # 🦉 SITE ADMIN (263), 🦉🦉 SITE SUPER ADMIN (280)
AUTHORIZED_SIGNER_ID = 395  # Agreement Signer

# Initialize GCS client
storage_client = storage.Client()

print(f"TESTING_MODE: {TESTING_MODE}")
if TESTING_MODE:
    print(f"All emails will be sent to: {TEST_EMAIL_OVERRIDE}")

# ============================================================================
# UTILS
# ============================================================================

def validate_hubspot_signature():
    print("DEBUG validate_hubspot_signature() called", flush=True)
    """
    Validate HubSpot request signature
    
    CRM CARDS use v2: SHA256(client_secret + method + url + body) as hex digest
    WEBHOOKS use v3: HMAC-SHA256(method + url + body + timestamp) as base64
    """
    print("DEBUG validate_hubspot_signature() called", flush=True)

    if not CLIENT_SECRET:
        app.logger.warning("CLIENT_SECRET not set, skipping signature validation")
        return True

    # Log the CLIENT_SECRET length (not the value)
    print(f"DEBUG CLIENT_SECRET length: {len(CLIENT_SECRET)}", flush=True)
    print(f"DEBUG CLIENT_SECRET starts with: {CLIENT_SECRET[:10]}...", flush=True)

    # Check signature version - CRM cards use v2
    signature_version = request.headers.get('X-HubSpot-Signature-Version', 'v2')
    signature = request.headers.get('X-HubSpot-Signature')
    
    print(f"DEBUG Signature version: {signature_version}", flush=True)
    
    if not signature:
        app.logger.error("Missing X-HubSpot-Signature header")
        return False

    method = request.method
    url = request.url.replace('http://', 'https://')
    body = request.get_data(as_text=True)

    # Cache body for later use
    from io import BytesIO
    request._cached_data = body
    request.environ['wsgi.input'] = BytesIO(body.encode('utf-8'))

    # ===================================================================
    # v2 SIGNATURE (CRM CARDS)
    # Format: SHA256(client_secret + method + url + body)
    # Result: hex digest (lowercase)
    # ===================================================================
    if signature_version == 'v2':
        source_string = f"{CLIENT_SECRET}{method}{url}"
        if body:
            source_string += body
        
        print(f"DEBUG === v2 SIGNATURE VALIDATION (CRM Cards) ===", flush=True)
        print(f"DEBUG Method: {method}", flush=True)
        print(f"DEBUG URL: {url}", flush=True)
        print(f"DEBUG Body length: {len(body)} bytes", flush=True)
        print(f"DEBUG Source string length: {len(source_string)}", flush=True)
        
        # Calculate expected signature (hex digest)
        expected_signature = hashlib.sha256(source_string.encode('utf-8')).hexdigest()
        
        print(f"DEBUG Received signature: {signature}", flush=True)
        print(f"DEBUG Expected signature: {expected_signature}", flush=True)
        
        # Compare (case-insensitive)
        if hmac.compare_digest(signature.lower(), expected_signature.lower()):
            print("DEBUG ✅ v2 Signature VALID", flush=True)
            return True
        else:
            print("DEBUG ❌ v2 Signature MISMATCH", flush=True)
            return False
    
    # ===================================================================
    # v3 SIGNATURE (WEBHOOKS)
    # Format: HMAC-SHA256(method + url + body + timestamp)
    # Result: base64 encoded
    # ===================================================================
    elif signature_version == 'v3':
        timestamp = request.headers.get('X-HubSpot-Request-Timestamp')
        if not timestamp:
            app.logger.error("Missing timestamp for v3 signature")
            return False
        
        # Validate timestamp (must be within 5 minutes)
        try:
            import time
            current_time = int(time.time() * 1000)
            request_time = int(timestamp)
            time_diff = abs(current_time - request_time)
            
            if time_diff > 300000:  # 5 minutes in milliseconds
                app.logger.error(f"Request timestamp too old: {time_diff}ms")
                return False
        except (ValueError, TypeError) as e:
            app.logger.error(f"Invalid timestamp: {e}")
            return False
        
        source_string = f"{method}{url}{body}{timestamp}"
        
        print(f"DEBUG === v3 SIGNATURE VALIDATION (Webhooks) ===", flush=True)
        print(f"DEBUG Method: {method}", flush=True)
        print(f"DEBUG URL: {url}", flush=True)
        print(f"DEBUG Timestamp: {timestamp}", flush=True)
        
        # Calculate expected signature (base64 HMAC)
        signature_bytes = hmac.new(
            CLIENT_SECRET.encode('utf-8'),
            source_string.encode('utf-8'),
            hashlib.sha256
        ).digest()
        expected_signature = base64.b64encode(signature_bytes).decode('utf-8')
        
        print(f"DEBUG Received signature: {signature[:20]}...", flush=True)
        print(f"DEBUG Expected signature: {expected_signature[:20]}...", flush=True)
        
        if hmac.compare_digest(signature, expected_signature):
            print("DEBUG ✅ v3 Signature VALID", flush=True)
            return True
        else:
            print("DEBUG ❌ v3 Signature MISMATCH", flush=True)
            return False
    
    else:
        app.logger.error(f"Unknown signature version: {signature_version}")
        return False

def upload_pdf_to_gcs(pdf_content, filename):
    """Upload PDF to GCS and return public URL (backup)"""
    try:
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(filename)

        blob.upload_from_string(
            pdf_content,
            content_type='application/pdf'
        )

        blob.make_public()
        url = blob.public_url

        print(f"DEBUG PDF uploaded to GCS (backup): {filename}", flush=True)
        print(f"DEBUG Public URL: {url}", flush=True)

        return url
    except Exception as e:
        app.logger.error(f"Error uploading to GCS: {str(e)}")
        return None


def upload_pdf_to_hubspot(pdf_content, certificate_name):
    """Upload PDF to HubSpot Files and return HubSpot URL"""
    if not HUBSPOT_ACCESS_TOKEN:
        app.logger.warning("HUBSPOT_ACCESS_TOKEN not set, skipping HubSpot upload")
        return None

    try:
        upload_url = "https://api.hubapi.com/files/v3/files"

        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}"
        }

        if not certificate_name.endswith('.pdf'):
            certificate_name = f"{certificate_name}.pdf"

        files = {
            'file': (certificate_name, pdf_content, 'application/pdf')
        }

        # SECURITY: Use PUBLIC_NOT_INDEXABLE for certificate PDFs
        data = {
            'options': json.dumps({"access": "PUBLIC_NOT_INDEXABLE"}),
            'folderPath': '/certificates'
        }

        print(f"DEBUG Uploading to HubSpot Files: {certificate_name}", flush=True)
        response = requests.post(upload_url, files=files, data=data, headers=headers, timeout=30)

        if response.status_code not in [200, 201]:
            app.logger.error(f"HubSpot upload failed: {response.status_code} - {response.text}")
            return None

        result = response.json()
        hubspot_url = result.get('url')

        if hubspot_url:
            print(f"DEBUG HubSpot upload successful: {hubspot_url}", flush=True)
            return hubspot_url
        else:
            app.logger.error(f"No URL in HubSpot response: {result}")
            return None

    except Exception as e:
        app.logger.error(f"HubSpot upload error: {str(e)}")
        return None


def download_pdf(pdf_url):
    """Download PDF from URL to temporary file"""
    try:
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()

        temp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
        temp_pdf.write(response.content)
        temp_pdf.close()

        print(f"DEBUG PDF downloaded to temp file: {temp_pdf.name}", flush=True)
        return temp_pdf.name
    except Exception as e:
        app.logger.error(f"Error downloading PDF: {str(e)}")
        raise


def convert_pdf_to_preview_image(pdf_path):
    """Convert first page of PDF to high-quality PNG"""
    try:
        print(f"DEBUG Converting PDF to image: {pdf_path}", flush=True)

        images = convert_from_path(
            pdf_path,
            first_page=1,
            last_page=1,
            dpi=300
        )

        if not images:
            raise Exception("No images generated from PDF")

        image = images[0]
        print(f"DEBUG Image generated: {image.width}x{image.height}px", flush=True)

        img_byte_arr = BytesIO()
        image.save(img_byte_arr, format='PNG', optimize=True, quality=95)
        img_byte_arr.seek(0)

        print(f"DEBUG PNG size: {len(img_byte_arr.getvalue())} bytes", flush=True)
        return img_byte_arr.getvalue()
    except Exception as e:
        app.logger.error(f"Error converting PDF to image: {str(e)}")
        raise
    finally:
        try:
            os.unlink(pdf_path)
            print(f"DEBUG Cleaned up temp file: {pdf_path}", flush=True)
        except Exception:
            pass


def upload_preview_to_hubspot(image_bytes, certificate_id):
    """Upload preview PNG to HubSpot Files"""
    if not HUBSPOT_ACCESS_TOKEN:
        app.logger.warning("HUBSPOT_ACCESS_TOKEN not set, skipping preview upload")
        return None

    try:
        file_name = f"{certificate_id}-preview.png"

        file_options = {
            'access': 'PUBLIC_NOT_INDEXABLE',
            'overwrite': False,
            'duplicateValidationStrategy': 'NONE',
            'duplicateValidationScope': 'EXACT_FOLDER'
        }

        files = {
            'file': (file_name, image_bytes, 'image/png'),
            'options': (None, json.dumps(file_options), 'application/json'),
            'folderPath': (None, '/certificate-previews', 'text/plain')
        }

        headers = {'Authorization': f'Bearer {HUBSPOT_ACCESS_TOKEN}'}

        print(f"DEBUG Uploading preview to HubSpot: {file_name}", flush=True)
        response = requests.post(
            'https://api.hubapi.com/files/v3/files',
            headers=headers,
            files=files,
            timeout=30
        )

        if response.status_code not in [200, 201]:
            app.logger.error(f"HubSpot preview upload failed: {response.status_code} - {response.text}")
            return None

        result = response.json()
        preview_url = result.get('url')

        if preview_url:
            print(f"DEBUG Preview uploaded: {preview_url}", flush=True)
            return preview_url
        else:
            app.logger.error(f"No URL in HubSpot preview response: {result}")
            return None

    except Exception as e:
        app.logger.error(f"Preview upload error: {str(e)}")
        return None


def create_or_get_contact(token, email, first_name, last_name, phone=None, company_id=None):
    """
    Create contact or get existing by email.
    Associates to company if company_id provided.
    Returns contact ID.
    """
    try:
        search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
        headers = get_headers(token)

        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }]
            }],
            "properties": ["firstname", "lastname", "email", "phone"],
            "limit": 1
        }

        print(f"DEBUG Searching for existing contact: {email}", flush=True)
        search_response = requests.post(search_url, headers=headers, json=search_body, timeout=30)

        if search_response.status_code == 200:
            search_data = search_response.json()
            if search_data.get('results'):
                contact = search_data['results'][0]
                contact_id = contact['id']
                print(f"DEBUG Found existing contact: {contact_id} ({email})", flush=True)

                if company_id:
                    associate_records(token, 'contact', contact_id, 'company', company_id)

                return contact_id

        # Create new contact
        contact_properties = {
            "email": email,
            "firstname": first_name or "",
            "lastname": last_name or ""
        }

        if phone:
            contact_properties["phone"] = phone

        print(f"DEBUG Creating new contact: {email}", flush=True)
        new_contact = create_contact(token, contact_properties)

        if new_contact:
            contact_id = new_contact['id']
            print(f"DEBUG Created new contact: {contact_id} ({email})", flush=True)

            if company_id:
                associate_records(token, 'contact', contact_id, 'company', company_id)

            return contact_id

        return None

    except Exception as e:
        print(f"ERROR creating/getting contact: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return None


def create_or_get_underwriter(token, underwriter_name):
    """
    Create underwriter company or get existing by name.
    Returns company ID.
    """
    try:
        print(f"DEBUG Searching for underwriter: {underwriter_name}", flush=True)
        existing = search_company_by_name(token, underwriter_name)

        if existing:
            underwriter_id = existing['id']
            print(f"DEBUG Found existing underwriter: {underwriter_id}", flush=True)

            company_type = existing.get('properties', {}).get('company_type', '')
            if 'Insurance Underwriter' not in company_type:
                print(f"DEBUG Updating company_type for {underwriter_id}", flush=True)
                update_company(token, underwriter_id, {
                    'company_type': 'Insurance Underwriter'
                })

            return underwriter_id

        # Create new underwriter
        print(f"DEBUG Creating new underwriter: {underwriter_name}", flush=True)
        new_underwriter = create_company(token, {
            'name': underwriter_name,
            'company_type': 'Insurance Underwriter'
        })

        if new_underwriter:
            underwriter_id = new_underwriter['id']
            print(f"DEBUG Created underwriter: {underwriter_id}", flush=True)
            return underwriter_id

        return None

    except Exception as e:
        print(f"ERROR creating/getting underwriter: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return None


def update_ticket_properties(token, ticket_id, properties):
    """
    Update ticket properties using HubSpot v3 API
    """
    try:
        url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
        headers = get_headers(token)

        payload = {
            "properties": properties
        }

        print(f"DEBUG Updating ticket {ticket_id} with properties: {properties}", flush=True)
        response = requests.patch(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 200:
            print(f"DEBUG ✅ Ticket properties updated successfully", flush=True)
            return True
        else:
            print(f"ERROR Failed to update ticket properties: {response.status_code} - {response.text}", flush=True)
            return False

    except Exception as e:
        print(f"ERROR updating ticket properties: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return False


def create_custom_object_association(token, from_object_type, from_id, to_object_type_id, to_id, type_id):
    """
    Create association between custom objects using HubSpot v4 Associations API batch endpoint
    
    Args:
        from_object_type: Type of source object (e.g., 'ticket')
        from_id: ID of source object
        to_object_type_id: FULL TYPE ID of target object (e.g., '2-2532422' for system)
        to_id: ID of target object
        type_id: Association type ID
    """
    try:
        url = f"https://api.hubapi.com/crm/v4/associations/{from_object_type}/{to_object_type_id}/batch/create"
        headers = get_headers(token)

        payload = {
            "inputs": [
                {
                    "from": {"id": str(from_id)},
                    "to": {"id": str(to_id)},
                    "types": [
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": type_id
                        }
                    ]
                }
            ]
        }

        print(f"DEBUG Creating {from_object_type}→{to_object_type_id} association: {from_id}→{to_id}", flush=True)
        print(f"DEBUG Using v4 batch endpoint: {url}", flush=True)
        print(f"DEBUG Association type ID: {type_id} (USER_DEFINED category)", flush=True)

        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code in [200, 201, 207]:
            result = response.json()
            if result.get('errors'):
                print(f"ERROR Association batch errors: {result['errors']}", flush=True)
                return False
            print(f"DEBUG ✅ Association created successfully", flush=True)
            return True
        else:
            print(f"ERROR Association failed: {response.status_code} - {response.text}", flush=True)
            return False

    except Exception as e:
        print(f"ERROR creating custom object association: {str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return False

# ============================================================================
# ROUTES
# ============================================================================

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy"}), 200


@app.route('/api/get-systems', methods=['POST', 'OPTIONS'])
def get_systems():
    """Get active security systems for a site"""
    print("DEBUG get_systems() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if hasattr(request, '_cached_data') and request._cached_data:
            data = json.loads(request._cached_data)
        else:
            data = request.get_json()

        if not data:
            app.logger.error("No data in request")
            return jsonify({"error": "No data provided"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        site_id = data.get('siteId')

        if not site_id:
            app.logger.error("No siteId in payload")
            return jsonify({"error": "No siteId provided"}), 400

        print(f"DEBUG Getting systems for site: {site_id}", flush=True)

        if not HUBSPOT_ACCESS_TOKEN:
            app.logger.error("HUBSPOT_ACCESS_TOKEN not set")
            return jsonify({"error": "HubSpot authentication not configured"}), 500

        assoc_url = f"https://api.hubapi.com/crm/v4/objects/company/{site_id}/associations/{SYSTEM_TYPE_ID}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        print(f"DEBUG Querying associations: {assoc_url}", flush=True)
        assoc_response = requests.get(assoc_url, headers=headers, timeout=30)

        if assoc_response.status_code != 200:
            app.logger.error(f"HubSpot associations error: {assoc_response.status_code} - {assoc_response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to get systems: {assoc_response.status_code}"
            }), 500

        assoc_data = assoc_response.json()

        if not assoc_data.get('results'):
            print("DEBUG No systems found for this site", flush=True)
            return jsonify({
                "success": False,
                "error": "No security systems found for this site. Certificates cannot be created for sites without an active security system.",
                "systems": []
            }), 200

        systems = []
        had_load_errors = False

        for assoc in assoc_data['results']:
            system_id = assoc.get('toObjectId')
            if not system_id:
                continue

            system_url = f"https://api.hubapi.com/crm/v3/objects/{SYSTEM_TYPE_ID}/{system_id}"
            params = {
                'properties': 'hs_object_id,name,system_address,current_status,category'
            }

            system_response = requests.get(system_url, headers=headers, params=params, timeout=30)

            if system_response.status_code != 200:
                had_load_errors = True
                print(f"DEBUG Failed to load system {system_id}: {system_response.status_code}", flush=True)
                continue

            system_data = system_response.json()
            props = system_data.get('properties', {})

            current_status = props.get('current_status', '')
            category = props.get('category', '')

            if current_status == 'Active' and category == 'Security':
                system_name = props.get('name') or props.get('system_address') or f"System {system_id}"

                systems.append({
                    'id': system_id,
                    'name': system_name
                })

                print(f"DEBUG Added system: {system_id} - {system_name}", flush=True)
            else:
                print(f"DEBUG Filtered out system {system_id}: status={current_status}, category={category}", flush=True)

        print(f"DEBUG Returning {len(systems)} active security systems", flush=True)

        if len(systems) == 0:
            if had_load_errors:
                return jsonify({
                    "success": False,
                    "error": "Security systems are associated with this site, but the system records could not be loaded from HubSpot. This may be a permissions issue or the records were deleted.",
                    "systems": []
                }), 500
            else:
                return jsonify({
                    "success": False,
                    "error": "No active security systems found for this site. Systems must have status='Active' and category='Security'.",
                    "systems": []
                }), 200

        return jsonify({
            "success": True,
            "systems": systems
        }), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to HubSpot"
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@app.route('/api/get-agreements', methods=['POST', 'OPTIONS'])
def get_agreements():
    """Get active agreements for a system"""
    print("DEBUG get_agreements() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if hasattr(request, '_cached_data') and request._cached_data:
            data = json.loads(request._cached_data)
        else:
            data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        system_id = data.get('systemId')
        site_id = data.get('siteId')

        if not system_id:
            return jsonify({"error": "No systemId provided"}), 400

        print(f"DEBUG Getting agreements for system: {system_id}", flush=True)

        if not HUBSPOT_ACCESS_TOKEN:
            return jsonify({"error": "HubSpot authentication not configured"}), 500

        assoc_url = f"https://api.hubapi.com/crm/v4/objects/{SYSTEM_TYPE_ID}/{system_id}/associations/{AGREEMENT_TYPE_ID}"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        print(f"DEBUG Querying associations: {assoc_url}", flush=True)
        assoc_response = requests.get(assoc_url, headers=headers, timeout=30)

        if assoc_response.status_code != 200:
            app.logger.error(f"HubSpot associations error: {assoc_response.status_code} - {assoc_response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to get agreements: {assoc_response.status_code}"
            }), 500

        assoc_data = assoc_response.json()

        system_agreements = assoc_data.get('results', [])
        using_fallback = False

        if not system_agreements and site_id:
            print("DEBUG No system-level agreements, trying site-level fallback", flush=True)
            site_assoc_url = f"https://api.hubapi.com/crm/v4/objects/company/{site_id}/associations/{AGREEMENT_TYPE_ID}"
            site_assoc_response = requests.get(site_assoc_url, headers=headers, timeout=30)

            if site_assoc_response.status_code == 200:
                site_assoc_data = site_assoc_response.json()
                system_agreements = site_assoc_data.get('results', [])
                using_fallback = True
                print(f"DEBUG Found {len(system_agreements)} site-level agreements", flush=True)

        if not system_agreements:
            print("DEBUG No agreements found at system or site level", flush=True)
            return jsonify({
                "success": False,
                "error": "No service agreements found for this site. Certificates cannot be created for sites without an associated service agreement.",
                "agreements": []
            }), 200

        agreements = []
        today = datetime.now().date()

        for assoc in system_agreements:
            agreement_id = assoc.get('toObjectId')

            if not agreement_id:
                continue

            agreement_url = f"https://api.hubapi.com/crm/v3/objects/{AGREEMENT_TYPE_ID}/{agreement_id}"
            params = {
                'properties': 'hs_object_id,name,hs_pipeline_stage,agreement_type,agreement_service_initiation_date'
            }

            agreement_response = requests.get(agreement_url, headers=headers, params=params, timeout=30)

            if agreement_response.status_code != 200:
                print(f"DEBUG Failed to load agreement {agreement_id}: {agreement_response.status_code}", flush=True)
                continue

            agreement_data = agreement_response.json()
            props = agreement_data.get('properties', {})

            pipeline_stage = props.get('hs_pipeline_stage', '')
            agreement_type = props.get('agreement_type', '')
            initiation_date_str = props.get('agreement_service_initiation_date', '')

            print(f"DEBUG Agreement {agreement_id}: stage={pipeline_stage}, type={agreement_type}, initiation={initiation_date_str}", flush=True)

            if pipeline_stage != '88538194':
                print(f"DEBUG Filtered out agreement {agreement_id}: not active stage", flush=True)
                continue

            valid_types = ['Services Agreement', 'ULC Fire Agreement']
            if agreement_type not in valid_types:
                print(f"DEBUG Filtered out agreement {agreement_id}: invalid type", flush=True)
                continue

            if initiation_date_str:
                try:
                    if 'T' in initiation_date_str:
                        initiation_date = datetime.fromisoformat(
                            initiation_date_str.replace('Z', '+00:00')
                        ).date()
                    else:
                        initiation_date = datetime.strptime(initiation_date_str, '%Y-%m-%d').date()

                    if initiation_date > today:
                        print(f"DEBUG Filtered out agreement {agreement_id}: future initiation date", flush=True)
                        continue
                except Exception as e:
                    print(f"DEBUG Error parsing date for agreement {agreement_id}: {e}", flush=True)
                    continue

            agreement_name = props.get('name') or f"Agreement {agreement_id}"

            agreements.append({
                'id': agreement_id,
                'name': agreement_name,
                'needsAssociation': using_fallback
            })

            print(f"DEBUG Added agreement: {agreement_id} - {agreement_name} (needsAssociation: {using_fallback})", flush=True)

        print(f"DEBUG Returning {len(agreements)} valid agreements (using_fallback: {using_fallback})", flush=True)

        if len(agreements) == 0:
            return jsonify({
                "success": False,
                "error": "No active service agreements found for this site. Certificates cannot be created for sites without an active 'Services Agreement' or 'ULC Fire Agreement' in the Active stage.",
                "agreements": []
            }), 200

        return jsonify({
            "success": True,
            "agreements": agreements,
            "usingFallback": using_fallback
        }), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to HubSpot"
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@app.route('/api/get-brokers', methods=['POST', 'OPTIONS'])
def get_brokers():
    """Get all insurance broker companies"""
    print("DEBUG get_brokers() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if not HUBSPOT_ACCESS_TOKEN:
            return jsonify({"error": "HubSpot authentication not configured"}), 500

        search_url = "https://api.hubapi.com/crm/v3/objects/companies/search"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "company_type",
                    "operator": "CONTAINS_TOKEN",
                    "value": "Insurance Broker"
                }]
            }],
            "properties": ["name"],
            "limit": 100
        }

        print("DEBUG Searching for broker companies", flush=True)
        search_response = requests.post(search_url, headers=headers, json=search_body, timeout=30)

        if search_response.status_code != 200:
            app.logger.error(f"HubSpot search error: {search_response.status_code} - {search_response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to search brokers: {search_response.status_code}"
            }), 500

        search_data = search_response.json()

        brokers = []
        for company in search_data.get('results', []):
            broker_id = company.get('id')
            broker_name = company.get('properties', {}).get('name', f"Broker {broker_id}")

            brokers.append({
                'id': broker_id,
                'name': broker_name
            })

        print(f"DEBUG Returning {len(brokers)} broker companies", flush=True)

        return jsonify({
            "success": True,
            "brokers": brokers
        }), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to HubSpot"
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@app.route('/api/get-underwriters', methods=['POST', 'OPTIONS'])
def get_underwriters():
    """Get all insurance underwriter companies"""
    print("DEBUG get_underwriters() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if not HUBSPOT_ACCESS_TOKEN:
            return jsonify({"error": "HubSpot authentication not configured"}), 500

        search_url = "https://api.hubapi.com/crm/v3/objects/companies/search"
        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "company_type",
                    "operator": "CONTAINS_TOKEN",
                    "value": "Insurance Underwriter"
                }]
            }],
            "properties": ["name"],
            "limit": 100
        }

        print("DEBUG Searching for underwriter companies", flush=True)
        search_response = requests.post(search_url, headers=headers, json=search_body, timeout=30)

        if search_response.status_code != 200:
            app.logger.error(f"HubSpot search error: {search_response.status_code} - {search_response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to search underwriters: {search_response.status_code}"
            }), 500

        search_data = search_response.json()

        underwriters = []
        for company in search_data.get('results', []):
            underwriter_id = company.get('id')
            underwriter_name = company.get('properties', {}).get('name', f"Underwriter {underwriter_id}")

            underwriters.append({
                'id': underwriter_id,
                'name': underwriter_name
            })

        print(f"DEBUG Returning {len(underwriters)} underwriter companies", flush=True)

        return jsonify({
            "success": True,
            "underwriters": underwriters
        }), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to HubSpot"
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@app.route('/api/get-broker-contacts', methods=['POST', 'OPTIONS'])
def get_broker_contacts():
    """Get all contacts associated with a broker company"""
    print("DEBUG get_broker_contacts() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if hasattr(request, '_cached_data') and request._cached_data:
            data = json.loads(request._cached_data)
        else:
            data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        broker_id = data.get('brokerId')

        if not broker_id:
            return jsonify({"error": "No brokerId provided"}), 400

        print(f"DEBUG Getting contacts for broker: {broker_id}", flush=True)

        if not HUBSPOT_ACCESS_TOKEN:
            return jsonify({"error": "HubSpot authentication not configured"}), 500

        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        assoc_url = f"https://api.hubapi.com/crm/v4/objects/company/{broker_id}/associations/contact"

        print(f"DEBUG Querying broker contacts: {assoc_url}", flush=True)
        assoc_response = requests.get(assoc_url, headers=headers, timeout=30)

        if assoc_response.status_code != 200:
            app.logger.error(f"HubSpot associations error: {assoc_response.status_code} - {assoc_response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to get broker contacts: {assoc_response.status_code}"
            }), 500

        assoc_data = assoc_response.json()

        if not assoc_data.get('results'):
            print("DEBUG No contacts found for this broker", flush=True)
            return jsonify({
                "success": False,
                "error": "No contacts found for this broker company. Please enter contact information manually or add contacts to this broker in HubSpot.",
                "contacts": []
            }), 200

        contacts = []

        for assoc in assoc_data['results']:
            contact_id = assoc.get('toObjectId')

            if not contact_id:
                continue

            contact_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
            params = {
                'properties': 'firstname,lastname,email,phone'
            }

            contact_response = requests.get(contact_url, headers=headers, params=params, timeout=30)

            if contact_response.status_code != 200:
                print(f"DEBUG Failed to load contact {contact_id}", flush=True)
                continue

            contact_data = contact_response.json()
            props = contact_data.get('properties', {})

            firstname = props.get('firstname', '')
            lastname = props.get('lastname', '')
            email = props.get('email', '')
            phone = props.get('phone', '')

            name = f"{firstname} {lastname}".strip() or email or f"Contact {contact_id}"

            contacts.append({
                'id': contact_id,
                'name': name,
                'email': email,
                'phone': phone
            })

            print(f"DEBUG Added broker contact: {contact_id} - {name}", flush=True)

        print(f"DEBUG Returning {len(contacts)} broker contacts", flush=True)

        if len(contacts) == 0:
            return jsonify({
                "success": False,
                "error": "No valid contacts found for this broker company. Please enter contact information manually.",
                "contacts": []
            }), 200

        return jsonify({
            "success": True,
            "contacts": contacts
        }), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to HubSpot"
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@app.route('/api/get-requestors', methods=['POST', 'OPTIONS'])
def get_requestors():
    """
    Get authorized certificate requestors for a site.

    STRICT FILTERING:
    - Only returns contacts who are:
      - Associated to Site as 🦉 SITE ADMIN (263) or 🦉🦉 SITE SUPER ADMIN (280), OR
      - Associated to Agreement as Signer (395)
    - NO fallback to all contacts
    - Returns success: False with clear error if none qualify
    """
    print("DEBUG get_requestors() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if hasattr(request, '_cached_data') and request._cached_data:
            data = json.loads(request._cached_data)
        else:
            data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        site_id = data.get('siteId')
        system_id = data.get('systemId')
        agreement_id = data.get('agreementId')

        if not site_id:
            return jsonify({"error": "No siteId provided"}), 400

        print(f"DEBUG Getting requestors for site: {site_id}, system: {system_id}, agreement: {agreement_id}", flush=True)

        if not HUBSPOT_ACCESS_TOKEN:
            return jsonify({"error": "HubSpot authentication not configured"}), 500

        headers = {
            "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        # Step 1: Get Site Admin/Super Admin contacts
        site_admin_contact_ids = set()

        site_assoc_url = f"https://api.hubapi.com/crm/v4/objects/company/{site_id}/associations/contact"
        print(f"DEBUG Querying site contacts: {site_assoc_url}", flush=True)

        site_assoc_response = requests.get(site_assoc_url, headers=headers, timeout=30)

        if site_assoc_response.status_code != 200:
            app.logger.error(f"HubSpot site associations error: {site_assoc_response.status_code} - {site_assoc_response.text}")
            return jsonify({
                "success": False,
                "error": f"Failed to get contacts for site: {site_assoc_response.status_code}",
                "requestors": []
            }), 500

        site_assoc_data = site_assoc_response.json()

        if not site_assoc_data.get('results'):
            print("DEBUG No contacts associated with this site", flush=True)
        else:
            for assoc in site_assoc_data['results']:
                contact_id = assoc.get('toObjectId')
                if not contact_id:
                    continue

                assoc_types = assoc.get('associationTypes', [])
                type_ids = [assoc_type.get('typeId') for assoc_type in assoc_types if assoc_type.get('typeId')]

                print(f"DEBUG Site contact {contact_id} type IDs: {type_ids}", flush=True)

                if any(type_id in AUTHORIZED_SITE_ADMIN_IDS for type_id in type_ids):
                    site_admin_contact_ids.add(contact_id)
                    print(f"DEBUG Contact {contact_id} authorized via Site Admin/Super Admin", flush=True)

        # Step 2: Get Agreement Signer contacts
        signer_contact_ids = set()

        if agreement_id:
            agreement_assoc_url = f"https://api.hubapi.com/crm/v4/objects/{AGREEMENT_TYPE_ID}/{agreement_id}/associations/contact"
            print(f"DEBUG Querying agreement contacts (signers): {agreement_assoc_url}", flush=True)

            agreement_assoc_response = requests.get(agreement_assoc_url, headers=headers, timeout=30)

            if agreement_assoc_response.status_code == 200:
                agreement_assoc_data = agreement_assoc_response.json()

                for assoc in agreement_assoc_data.get('results', []):
                    contact_id = assoc.get('toObjectId')
                    if not contact_id:
                        continue

                    assoc_types = assoc.get('associationTypes', [])
                    type_ids = [assoc_type.get('typeId') for assoc_type in assoc_types if assoc_type.get('typeId')]

                    print(f"DEBUG Agreement contact {contact_id} type IDs: {type_ids}", flush=True)

                    if any(type_id == AUTHORIZED_SIGNER_ID for type_id in type_ids):
                        signer_contact_ids.add(contact_id)
                        print(f"DEBUG Contact {contact_id} authorized via Agreement Signer", flush=True)
            else:
                app.logger.error(f"Agreement associations error: {agreement_assoc_response.status_code} - {agreement_assoc_response.text}")

        # Step 3: Combine authorized contacts
        authorized_contact_ids = site_admin_contact_ids.union(signer_contact_ids)

        print(f"DEBUG Site Admin/Super Admin IDs: {sorted(list(site_admin_contact_ids))}", flush=True)
        print(f"DEBUG Signer IDs: {sorted(list(signer_contact_ids))}", flush=True)
        print(f"DEBUG Authorized contact IDs (union): {sorted(list(authorized_contact_ids))}", flush=True)

        if not authorized_contact_ids:
            return jsonify({
                "success": False,
                "error": (
                    "No authorized requestors found for this site. "
                    "To request a certificate, a contact must either:\n"
                    "• Be associated to the site as 🦉 SITE ADMIN or 🦉🦉 SITE SUPER ADMIN, or\n"
                    "• Be associated to the service agreement as a Signer."
                ),
                "requestors": []
            }), 200

        # Step 4: Load contact records
        requestors = []

        for contact_id in authorized_contact_ids:
            contact_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
            params = {'properties': 'firstname,lastname,email'}

            contact_response = requests.get(contact_url, headers=headers, params=params, timeout=30)

            if contact_response.status_code != 200:
                print(f"DEBUG Failed to load contact {contact_id}: {contact_response.status_code}", flush=True)
                continue

            contact_data = contact_response.json()
            props = contact_data.get('properties', {})

            firstname = props.get('firstname', '')
            lastname = props.get('lastname', '')
            email = props.get('email', '')

            name = f"{firstname} {lastname}".strip() or email or f"Contact {contact_id}"

            requestors.append({
                'id': contact_id,
                'name': name,
                'email': email
            })

            print(f"DEBUG Added requestor: {contact_id} - {name}", flush=True)

        print(f"DEBUG Returning {len(requestors)} requestors (strict authorization)", flush=True)

        if len(requestors) == 0:
            return jsonify({
                "success": False,
                "error": "Authorized requestors exist but their contact records could not be loaded from HubSpot. This may be a permissions issue.",
                "requestors": []
            }), 500

        return jsonify({
            "success": True,
            "requestors": requestors
        }), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to HubSpot",
            "requestors": []
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": "Internal server error",
            "requestors": []
        }), 500


@app.route('/api/generate-certificate', methods=['POST', 'OPTIONS'])
def generate_certificate():
    """Generate certificate by calling WebMerge and uploading to Drive + GCS + HubSpot"""
    print("DEBUG generate_certificate() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        if hasattr(request, '_cached_data') and request._cached_data:
            data = json.loads(request._cached_data)
        else:
            data = request.get_json()

        if not data:
            app.logger.error("No data in request")
            return jsonify({"error": "No data provided"}), 400

        if isinstance(data, str):
            data = json.loads(data)

        certificate_data = data.get('certificateData', {})

        if not certificate_data:
            app.logger.error(f"No certificateData in payload. Received: {data}")
            return jsonify({"error": "No certificateData provided"}), 400

        required_fields = ['requestorName', 'brokerCompany', 'brokerContact', 'brokerEmail']
        missing_fields = [field for field in required_fields if not certificate_data.get(field)]

        if missing_fields:
            return jsonify({
                "error": f"Missing required fields: {', '.join(missing_fields)}"
            }), 400

        # Prepare WebMerge data
        webmerge_params = {
            'provider_name': 'Provident Security Corp',
            'provider_address': '1055 West Hastings Street, Suite 300',
            'provider_city': 'Vancouver',
            'provider_state': 'BC',
            'provider_zip': 'V6E 2E9',
            'provider_phone': '(604) 254-9734',
            'site_name': certificate_data.get('siteName', ''),
            'site_address': certificate_data.get('siteAddress', ''),
            'site_city': certificate_data.get('siteCity', ''),
            'site_state': certificate_data.get('siteState', ''),
            'site_zip': certificate_data.get('siteZip', ''),
            'system_name': certificate_data.get('systemName', ''),
            'account_number': certificate_data.get('accountNumber', ''),
            'agreement_number': certificate_data.get('agreementNumber', 'N/A'),
            'requestor_name': certificate_data.get('requestorName', ''),
            'broker_company': certificate_data.get('brokerCompany', ''),
            'broker_contact': certificate_data.get('brokerContact', ''),
            'broker_email': certificate_data.get('brokerEmail', ''),
            'certificate_date': certificate_data.get('certificateDate', ''),
        }

        # Call WebMerge
        print("DEBUG Calling WebMerge API...", flush=True)
        response = requests.post(
            WEBMERGE_URL,
            data=webmerge_params,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30
        )

        if response.status_code not in [200, 201, 204]:
            app.logger.error(f"WebMerge error: {response.status_code} - {response.text}")
            return jsonify({
                "success": False,
                "error": f"WebMerge API error: {response.status_code}"
            }), 500

        pdf_content = response.content
        print(f"DEBUG PDF received from WebMerge: {len(pdf_content)} bytes", flush=True)

        certificate_id = str(uuid.uuid4())
        filename = f"certificates/{certificate_id}.pdf"

        # Upload to GCS (backup)
        gcs_url = upload_pdf_to_gcs(pdf_content, filename)

        # Upload to Google Drive (primary)
        drive_url = None
        generated_certs_folder_id = os.environ.get('GENERATED_CERTIFICATES_FOLDER_ID')
        site_folder_id = certificate_data.get('siteFolderId')
        site_id = certificate_data.get('siteId', 'unknown')

        if generated_certs_folder_id:
            try:
                print(f"DEBUG Site ID: {site_id}", flush=True)
                print(f"DEBUG Master Certificates Folder ID: {generated_certs_folder_id}", flush=True)

                pst = timezone(timedelta(hours=-8))
                timestamp = datetime.now(pst).strftime('%Y%m%d-%H%M')
                drive_filename = f"Certificate_{site_id}_{timestamp}.pdf"

                drive_result = upload_file_to_folder(
                    pdf_content,
                    generated_certs_folder_id,
                    drive_filename,
                    'application/pdf'
                )

                drive_url = drive_result.get('web_view_link')
                file_id = drive_result.get('file_id')
                print(f"DEBUG Certificate uploaded to Drive: {drive_url}", flush=True)

                # Create shortcut in site folder
                if site_folder_id and file_id:
                    try:
                        site_shortcuts_folder = get_or_create_folder(site_folder_id, "Certificates")
                        shortcut_id = create_shortcut(file_id, drive_filename, site_shortcuts_folder)
                        if shortcut_id:
                            print(f"DEBUG Shortcut created in site folder: {shortcut_id}", flush=True)
                    except Exception as e:
                        print(f"WARNING: Could not create shortcut in site folder: {str(e)}", flush=True)

            except Exception as e:
                print(f"ERROR uploading to Drive: {str(e)}", flush=True)

        # Upload to HubSpot Files
        site_name = certificate_data.get('siteName', 'Certificate').replace(' ', '_')
        certificate_name = f"certificate-{site_name}-{certificate_id[:8]}"
        hubspot_url = upload_pdf_to_hubspot(pdf_content, certificate_name)

        # Generate preview image
        preview_image_url = None
        try:
            print("DEBUG Starting preview generation...", flush=True)

            temp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            temp_pdf.write(pdf_content)
            temp_pdf.close()
            print(f"DEBUG PDF written to temp file: {temp_pdf.name}", flush=True)

            image_bytes = convert_pdf_to_preview_image(temp_pdf.name)
            preview_image_url = upload_preview_to_hubspot(image_bytes, certificate_id)

            if preview_image_url:
                print(f"DEBUG ✅ Preview generation complete: {preview_image_url}", flush=True)

        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            app.logger.error(f"Preview generation failed: {str(e)}")
            print(f"ERROR: Preview generation failed: {str(e)}", flush=True)

        # Return URLs
        primary_url = drive_url or gcs_url

        response_data = {
            "success": True,
            "certificate_id": certificate_id,
            "pdf_url": primary_url,
            "message": "Certificate generated successfully"
        }

        if hubspot_url:
            response_data["hubspot_url"] = hubspot_url
        if preview_image_url:
            response_data["preview_image_url"] = preview_image_url
        if drive_url:
            response_data["drive_url"] = drive_url
        if gcs_url:
            response_data["gcs_backup_url"] = gcs_url

        return jsonify(response_data), 200

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Request error: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to connect to WebMerge"
        }), 500
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": "Internal server error"
        }), 500


@app.route('/api/send-certificate-email', methods=['POST', 'OPTIONS'])
def send_certificate_email():
    """
    Send certificate via SMTP email with HTML formatting, validation, and HubSpot integration
    """
    print("DEBUG send_certificate_email() called", flush=True)

    if request.method == 'OPTIONS':
        return '', 204

    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401

    try:
        data = request.json or {}

        # Extract data
        ticket_id = data.get('ticketId')
        certificate_id = data.get('certificateId')
        certificate_pdf_url = data.get('certificatePdfUrl')
        broker_email = data.get('brokerEmail')
        broker_name = data.get('brokerName')
        broker_company_name = data.get('brokerCompany')
        site_address = data.get('siteAddress', '')
        site_id = data.get('siteId')
        system_id = data.get('systemId')
        agreement_id = data.get('agreementId')
        broker_id = data.get('brokerId')
        broker_contact_id = data.get('brokerContactId')
        requestor_id = data.get('requestorId')
        underwriter_id = data.get('underwriterId')
        preview_image_url = data.get('previewImageUrl')

        # Manual entry data
        manual_requestor_first = data.get('manualRequestorFirstName')
        manual_requestor_last = data.get('manualRequestorLastName')
        manual_requestor_email = data.get('manualRequestorEmail')
        manual_requestor_phone = data.get('manualRequestorPhone')

        manual_broker_first = data.get('manualBrokerFirstName')
        manual_broker_last = data.get('manualBrokerLastName')
        manual_broker_email = data.get('manualBrokerEmail')
        manual_broker_phone = data.get('manualBrokerPhone')

        manual_underwriter_name = data.get('manualUnderwriterName')

        # SMTP config
        SMTP_HOST = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
        SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
        SMTP_USER = os.environ.get('SMTP_USER')
        SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')
        SMTP_FROM_EMAIL = os.environ.get('SMTP_FROM_EMAIL', 'customerservice@providentsecurity.ca')
        SMTP_FROM_NAME = os.environ.get('SMTP_FROM_NAME', 'Provident Security - Customer Service')

        print(f"DEBUG Sending to: {broker_email}, Ticket: {ticket_id}", flush=True)

        # TESTING MODE override
        original_broker_email = broker_email
        if TESTING_MODE and broker_email:
            broker_email = TEST_EMAIL_OVERRIDE
            print(f"DEBUG TESTING MODE: Email redirected from {original_broker_email} to {broker_email}", flush=True)

        # Debug: Log all required fields
        print(f"DEBUG VALIDATION CHECK:", flush=True)
        print(f"  ticket_id: {ticket_id}", flush=True)
        print(f"  broker_email: {broker_email}", flush=True)
        print(f"  broker_name: {broker_name}", flush=True)
        print(f"  certificate_pdf_url: {certificate_pdf_url}", flush=True)

        # Validate required fields
        if not all([ticket_id, broker_email, broker_name, certificate_pdf_url]):
            return jsonify({
                "success": False,
                "error": "Missing required fields"
            }), 400

        if not SMTP_USER or not SMTP_PASSWORD:
            return jsonify({"success": False, "error": "SMTP not configured"}), 500

        # Validate site is active
        if site_id:
            site_url = f"https://api.hubapi.com/crm/v3/objects/companies/{site_id}"
            site_params = {'properties': 'current_status,name'}
            headers = get_headers(HUBSPOT_ACCESS_TOKEN)

            site_response = requests.get(site_url, headers=headers, params=site_params, timeout=30)

            if site_response.status_code == 200:
                site_data = site_response.json()
                site_name = site_data.get('properties', {}).get('name', 'Unknown')
                current_status = site_data.get('properties', {}).get('current_status', '')

                if current_status != 'Active':
                    error_msg = f"Site '{site_name}' is not active (status: {current_status}). Only active sites can receive certificates."
                    print(f"ERROR {error_msg}", flush=True)
                    return jsonify({"success": False, "error": error_msg}), 400

        # Broker validation & creation
        validated_broker_id = broker_id
        broker_domain = None

        if broker_company_name and not broker_id:
            existing_broker = search_company_by_name(HUBSPOT_ACCESS_TOKEN, broker_company_name)

            if existing_broker:
                validated_broker_id = existing_broker['id']
                broker_domain = existing_broker.get('properties', {}).get('domain')

                company_type = existing_broker.get('properties', {}).get('company_type', '')
                if 'Insurance Broker' not in company_type:
                    update_company(HUBSPOT_ACCESS_TOKEN, validated_broker_id, {
                        'company_type': 'Insurance Broker'
                    })
            else:
                new_broker = create_company(HUBSPOT_ACCESS_TOKEN, {
                    'name': broker_company_name,
                    'company_type': 'Insurance Broker'
                })
                if new_broker:
                    validated_broker_id = new_broker['id']

        elif broker_id:
            broker_domain = get_company_property(HUBSPOT_ACCESS_TOKEN, broker_id, 'domain')

        # Contact creation (manual entries)
        validated_requestor_id = requestor_id
        validated_broker_contact_id = broker_contact_id
        validated_underwriter_id = underwriter_id

        if not requestor_id and manual_requestor_email:
            validated_requestor_id = create_or_get_contact(
                HUBSPOT_ACCESS_TOKEN,
                manual_requestor_email,
                manual_requestor_first,
                manual_requestor_last,
                manual_requestor_phone,
                site_id
            )

        if not broker_contact_id and manual_broker_email:
            validated_broker_contact_id = create_or_get_contact(
                HUBSPOT_ACCESS_TOKEN,
                manual_broker_email,
                manual_broker_first,
                manual_broker_last,
                manual_broker_phone,
                validated_broker_id
            )

        if not underwriter_id and manual_underwriter_name:
            validated_underwriter_id = create_or_get_underwriter(
                HUBSPOT_ACCESS_TOKEN,
                manual_underwriter_name
            )

        # Domain validation
        if broker_domain and original_broker_email:
            if not check_domain_match(original_broker_email, broker_domain):
                email_domain = extract_domain_from_email(original_broker_email)

                escalation_note = f"""<h3>⚠️ CERTIFICATE ESCALATION REQUIRED</h3>
<p><strong>Contact Email Domain Mismatch Detected</strong></p>
<p><strong>Broker Company:</strong> {broker_company_name}<br>
<strong>Broker Domain:</strong> {broker_domain}</p>
<p><strong>Contact Name:</strong> {broker_name}<br>
<strong>Contact Email:</strong> {original_broker_email}<br>
<strong>Contact Domain:</strong> {email_domain}</p>
<p><strong style="color: red;">ACTION REQUIRED:</strong><br>
Please verify this is the correct contact before sending the certificate.</p>"""

                create_note_on_ticket(HUBSPOT_ACCESS_TOKEN, ticket_id, escalation_note)

                return jsonify({
                    "success": False,
                    "error": "Domain mismatch detected. Escalation note created. Manager review required.",
                    "escalation": True,
                    "details": {
                        "broker_domain": broker_domain,
                        "contact_email_domain": email_domain
                    }
                }), 400

        # Send email
        certificate_number = certificate_id[:8] if certificate_id else ''
        subject = f"Provident Security Monitoring Certificate #{certificate_number}"

        html_body = f"""<html>
<body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
<p>Dear {broker_name},</p>
<p>Please find attached the current Security Monitoring Certificate for Provident services at <strong>{site_address}</strong>.</p>
<p>Should you have any questions, please contact us 24/7 by return email or by calling 604.664.1087</p>
<p>Thank you.</p>
<p>Provident Security<br>Customer Service Team</p>
</body>
</html>"""

        text_body = f"""Dear {broker_name},

Please find attached the current Security Monitoring Certificate for Provident services at {site_address}.

Should you have any questions, please contact us 24/7 by return email or by calling 604.664.1087

Thank you.

Provident Security
Customer Service Team"""

        msg = MIMEMultipart('alternative')
        msg['From'] = f"{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>"
        msg['To'] = broker_email
        msg['Subject'] = subject

        msg.attach(MIMEText(text_body, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))

        # Attach PDF
        pdf_response = requests.get(certificate_pdf_url, timeout=30)
        if pdf_response.status_code == 200:
            part = MIMEBase('application', 'pdf')
            part.set_payload(pdf_response.content)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename='certificate.pdf')
            msg.attach(part)

        # Send via SMTP
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        print(f"DEBUG Email sent successfully to {broker_email}", flush=True)

        # Update ticket properties
        if HUBSPOT_ACCESS_TOKEN:
            drive_link = data.get('driveUrl', certificate_pdf_url)
            # Use current time instead of midnight so each send creates a unique timestamp
            
            current_time = datetime.now(timezone.utc)

            ticket_update_props = {
                'certificate_sent_date': str(int(current_time.timestamp() * 1000)),
                'certificate_pdf_url': drive_link
            }

            update_ticket_properties(HUBSPOT_ACCESS_TOKEN, ticket_id, ticket_update_props)

        # Create notes with PORTAL-AGNOSTIC links
        if HUBSPOT_ACCESS_TOKEN:
            headers = get_headers(HUBSPOT_ACCESS_TOKEN)
            csr_name = data.get('userName', 'Customer Service Team')

            # Get requestor info
            requestor_name = "Unknown"
            requestor_link = "Unknown"
            final_requestor_id = validated_requestor_id or requestor_id

            if final_requestor_id:
                contact_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{final_requestor_id}"
                contact_response = requests.get(contact_url, headers=headers, params={'properties': 'firstname,lastname'}, timeout=30)
                if contact_response.status_code == 200:
                    props = contact_response.json().get('properties', {})
                    requestor_name = f"{props.get('firstname', '')} {props.get('lastname', '')}".strip() or "Unknown"
                    requestor_link = f'<a href="https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-1/{final_requestor_id}" target="_blank">{requestor_name}</a>'

            # Broker contact link
            broker_contact_link = broker_name
            final_broker_contact_id = validated_broker_contact_id or broker_contact_id
            if final_broker_contact_id:
                broker_contact_link = f'<a href="https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-1/{final_broker_contact_id}" target="_blank">{broker_name}</a>'

            # Broker company link
            broker_company_link = broker_company_name
            final_broker_id = validated_broker_id or broker_id
            if final_broker_id:
                broker_company_link = f'<a href="https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/record/0-2/{final_broker_id}" target="_blank">{broker_company_name}</a>'

            cert_date = datetime.now().strftime('%B %d, %Y at %I:%M %p PST')
            drive_link = data.get('driveUrl', certificate_pdf_url)

            note_html = f"""<h3>SECURITY MONITORING CERTIFICATE GENERATED</h3>
<p><strong>Certificate #{certificate_id[:8]}</strong> was generated on {cert_date}</p>
<table style="border-collapse: collapse; width: 100%;">
<tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>CSR:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{csr_name}</td></tr>
<tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Requestor:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{requestor_link}</td></tr>
<tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Broker Company:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{broker_company_link}</td></tr>
<tr><td style="padding: 8px; border: 1px solid #ddd;"><strong>Broker Contact:</strong></td><td style="padding: 8px; border: 1px solid #ddd;">{broker_contact_link}</td></tr>
</table>
<p><strong>Certificate #{certificate_id[:8]} was generated and added to SITE folder.</strong></p>
<p><a href="{drive_link}" target="_blank">View Certificate in Google Drive</a></p>"""

            # Create note on ticket
            try:
                create_note_on_ticket(HUBSPOT_ACCESS_TOKEN, ticket_id, note_html)
            except Exception as e:
                print(f"WARNING: Could not create note on ticket: {str(e)}", flush=True)

            # Create note on site
            if site_id:
                try:
                    engagement_url = "https://api.hubapi.com/engagements/v1/engagements"
                    engagement_data = {
                        "engagement": {
                            "active": True,
                            "type": "NOTE",
                            "timestamp": int(datetime.now().timestamp() * 1000)
                        },
                        "associations": {"companyIds": [int(site_id)]},
                        "metadata": {"body": note_html}
                    }
                    requests.post(engagement_url, headers=headers, json=engagement_data, timeout=30)
                except Exception as e:
                    print(f"WARNING: Could not create note on company: {str(e)}", flush=True)

            # Create associations
            try:
                print("DEBUG ========== STARTING ASSOCIATIONS ==========", flush=True)

                if site_id:
                    associate_records(HUBSPOT_ACCESS_TOKEN, 'ticket', ticket_id, 'company', site_id, SITE_ASSOCIATION_TYPE_ID)

                if validated_underwriter_id:
                    associate_records(HUBSPOT_ACCESS_TOKEN, 'company', validated_underwriter_id, 'ticket', ticket_id, 486)

                if final_broker_id:
                    associate_records(HUBSPOT_ACCESS_TOKEN, 'ticket', ticket_id, 'company', final_broker_id, BROKER_COMPANY_ASSOCIATION_TYPE_ID)

                if system_id:
                    create_custom_object_association(HUBSPOT_ACCESS_TOKEN, 'ticket', ticket_id, SYSTEM_TYPE_ID, system_id, SYSTEM_ASSOCIATION_TYPE_ID)

                if agreement_id:
                    create_custom_object_association(HUBSPOT_ACCESS_TOKEN, 'ticket', ticket_id, AGREEMENT_TYPE_ID, agreement_id, AGREEMENT_ASSOCIATION_TYPE_ID)

                if final_broker_contact_id:
                    associate_records(HUBSPOT_ACCESS_TOKEN, 'ticket', ticket_id, 'contact', final_broker_contact_id, BROKER_CONTACT_ASSOCIATION_TYPE_ID)

                if final_requestor_id:
                    associate_records(HUBSPOT_ACCESS_TOKEN, 'ticket', ticket_id, 'contact', final_requestor_id, 482)

                print("DEBUG ========== ALL ASSOCIATIONS COMPLETED ==========", flush=True)

            except Exception as e:
                print(f"ERROR Association error: {str(e)}", flush=True)
                import traceback
                traceback.print_exc()

        return jsonify({
            "success": True,
            "message": "Certificate sent successfully"
        }), 200

    except Exception as e:
        error_details = traceback.format_exc()
        print(f"ERROR FULL TRACEBACK:", flush=True)
        print(error_details, flush=True)
        app.logger.error(f"Unexpected error: {str(e)}")
        app.logger.error(error_details)
        return jsonify({
            "success": False,
            "error": "Internal server error",
            "details": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
# Updated Mon 17 Nov 2025 v6.13 CORRECTED - Portal-agnostic + Strict Requestor Filtering + Security Improvements

# ============================================================
# CERTIFICATE GENERATION V2 - WITH VALIDATION & ASSEMBLY
# ============================================================
# Added: 2025-11-18
# Uses CertificateEngine for full HubSpot data fetching,
# validation, device grouping, and field assembly
# ============================================================

@app.route('/api/generate-certificate-v2', methods=['POST', 'OPTIONS'])
def generate_certificate_v2():
    """Generate certificate using full certificate engine"""
    print("DEBUG generate_certificate_v2() called", flush=True)
    
    if request.method == 'OPTIONS':
        return '', 204
    
    if not validate_hubspot_signature():
        return jsonify({"error": "Invalid signature"}), 401
    
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No data provided"}), 400
        
        # Extract IDs (required)
        agreement_id = data.get('agreementId')
        system_id = data.get('systemId')
        site_id = data.get('siteId')
        
        # Extract user inputs (optional)
        broker_email = data.get('brokerEmail', '')
        requestor_name = data.get('requestorName', '')
        broker_company = data.get('brokerCompany', '')
        broker_contact = data.get('brokerContact', '')
        site_folder_id = data.get('siteFolderId')
        
        # Validate required IDs
        if not all([agreement_id, system_id, site_id]):
            return jsonify({
                "error": "Missing required IDs: agreementId, systemId, siteId"
            }), 400
        
        # Initialize certificate engine
        from services import CertificateEngine
        
        engine = CertificateEngine(
            hubspot_token=HUBSPOT_ACCESS_TOKEN,
            portal_id=HUBSPOT_PORTAL_ID,
            system_type_id=SYSTEM_TYPE_ID,
            agreement_type_id=AGREEMENT_TYPE_ID
        )
        
        # Generate certificate data (validates, fetches, assembles)
        print("DEBUG Generating certificate data with engine...", flush=True)
        webmerge_params = engine.generate_certificate_data(
            agreement_id=agreement_id,
            system_id=system_id,
            site_id=site_id,
            broker_email=broker_email,
            requestor_name=requestor_name
        )
        
        # Add broker info (not in HubSpot data)
        webmerge_params['broker_company'] = broker_company
        webmerge_params['broker_contact'] = broker_contact
        webmerge_params['broker_email'] = broker_email
        webmerge_params['requestor_name'] = requestor_name
        
        # Call WebMerge
        print("DEBUG Calling WebMerge API...", flush=True)
        print(f"DEBUG CERTIFICATE_TimeStamp value: {webmerge_params.get('CERTIFICATE_TimeStamp')}", flush=True)
        response = requests.post(
            WEBMERGE_URL,
            json=webmerge_params,  # Use JSON, not form data
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        
        if response.status_code not in [200, 201, 204]:
            app.logger.error(f"WebMerge error: {response.status_code} - {response.text}")
            return jsonify({
                "success": False,
                "error": f"WebMerge API error: {response.status_code}",
                "details": response.text
            }), 500
        
        pdf_content = response.content
        print(f"DEBUG PDF received: {len(pdf_content)} bytes", flush=True)
        
        certificate_id = webmerge_params['CERTIFICATE_Number']
        filename = f"certificates/{certificate_id}.pdf"
        
        # Upload to GCS (backup)
        gcs_url = upload_pdf_to_gcs(pdf_content, filename)
        
        # Upload to Google Drive (primary)
        drive_url = None
        generated_certs_folder_id = os.environ.get('GENERATED_CERTIFICATES_FOLDER_ID')
        
        if generated_certs_folder_id:
            try:
                pst = timezone(timedelta(hours=-8))
                timestamp = datetime.now(pst).strftime('%Y%m%d-%H%M')
                drive_filename = f"Certificate_{site_id}_{timestamp}.pdf"
                
                drive_result = upload_file_to_folder(
                    pdf_content,
                    generated_certs_folder_id,
                    drive_filename,
                    'application/pdf'
                )
                
                drive_url = drive_result.get('web_view_link')
                file_id = drive_result.get('file_id')
                print(f"DEBUG Certificate uploaded to Drive: {drive_url}", flush=True)
                
                # Create shortcut in site folder
                if site_folder_id and file_id:
                    try:
                        site_shortcuts_folder = get_or_create_folder(site_folder_id, "Certificates")
                        shortcut_id = create_shortcut(file_id, drive_filename, site_shortcuts_folder)
                        if shortcut_id:
                            print(f"DEBUG Shortcut created: {shortcut_id}", flush=True)
                    except Exception as e:
                        print(f"WARNING: Shortcut creation failed: {str(e)}", flush=True)
            
            except Exception as e:
                print(f"ERROR uploading to Drive: {str(e)}", flush=True)
        
        # Upload to HubSpot Files
        site_name = webmerge_params.get('SITE_Name', 'Certificate').replace(' ', '_')
        certificate_name = f"certificate-{site_name}-{certificate_id.replace('-', '_')}"
        hubspot_url = upload_pdf_to_hubspot(pdf_content, certificate_name)
        
        # Generate preview image
        preview_image_url = None
        try:
            print("DEBUG Starting preview generation...", flush=True)
            temp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            temp_pdf.write(pdf_content)
            temp_pdf.close()
            
            image_bytes = convert_pdf_to_preview_image(temp_pdf.name)
            preview_image_url = upload_preview_to_hubspot(image_bytes, certificate_id)
            
            if preview_image_url:
                print(f"DEBUG ✅ Preview generated: {preview_image_url}", flush=True)
        
        except Exception as e:
            app.logger.error(f"Preview generation failed: {str(e)}")
            print(f"ERROR: Preview failed: {str(e)}", flush=True)
        
        # Return response
        primary_url = drive_url or gcs_url
        
        response_data = {
            "success": True,
            "certificate_id": certificate_id,
            "certificate_number": webmerge_params['CERTIFICATE_Number'],
            "pdf_url": primary_url,
            "message": "Certificate generated successfully with validation"
        }
        
        if hubspot_url:
            response_data["hubspot_url"] = hubspot_url
        if preview_image_url:
            response_data["preview_image_url"] = preview_image_url
        if drive_url:
            response_data["drive_url"] = drive_url
        if gcs_url:
            response_data["gcs_backup_url"] = gcs_url
        
        return jsonify(response_data), 200
    
    except ValueError as e:
        # Validation errors from certificate engine
        app.logger.error(f"Validation error: {str(e)}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400
    
    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": "Internal server error",
            "details": str(e)
        }), 500

# ============================================================
# END CERTIFICATE GENERATION V2
# ============================================================
