"""
Services package for certificate backend
"""

from .google_drive import (
    get_drive_service,
    get_or_create_folder,
    upload_file_to_folder,
    list_folder_contents,
    delete_file,
    create_shortcut
)

from .hubspot_api import (
    get_headers,
    search_company_by_name,
    create_company,
    update_company,
    create_contact,
    associate_records,
    create_note_on_ticket,
    get_company_property,
    extract_domain_from_email,
    check_domain_match
)

__all__ = [
    # Google Drive
    'get_drive_service',
    'get_or_create_folder',
    'upload_file_to_folder',
    'list_folder_contents',
    'delete_file',
    # HubSpot
    'get_headers',
    'search_company_by_name',
    'create_company',
    'update_company',
    'create_contact',
    'associate_records',
    'create_note_on_ticket',
    'get_company_property',
    'extract_domain_from_email',
    'check_domain_match'
]

from .certificate_engine import CertificateEngine

__all__.append('CertificateEngine')
