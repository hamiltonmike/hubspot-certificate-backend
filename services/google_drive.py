"""
Google Drive Service Module
Reusable functions for Google Drive operations
"""

import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload
from googleapiclient.errors import HttpError

# Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_drive_service():
    """
    Authenticate and return Google Drive service instance
    """
    try:
        # Get credentials from environment variable
        creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if not creds_json:
            raise Exception("GOOGLE_DRIVE_CREDENTIALS environment variable not set")
        
        # Parse JSON credentials
        creds_dict = json.loads(creds_json)
        
        # Create credentials
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=SCOPES
        )
        
        # Build and return service
        service = build('drive', 'v3', credentials=credentials)
        return service
        
    except Exception as e:
        print(f"ERROR: Failed to authenticate with Google Drive: {str(e)}")
        raise

def get_or_create_folder(parent_folder_id, folder_name):
    """
    Get folder ID if exists, or create it if it doesn't
    
    Args:
        parent_folder_id: ID of parent folder
        folder_name: Name of folder to find/create
        
    Returns:
        Folder ID (string)
    """
    service = get_drive_service()
    
    try:
        # Search for existing folder
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        
        if files:
            # Folder exists
            folder_id = files[0]['id']
            print(f"DEBUG Found existing folder '{folder_name}': {folder_id}")
            return folder_id
        
        # Folder doesn't exist - create it
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        
        folder = service.files().create(
            body=file_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        folder_id = folder.get('id')
        print(f"DEBUG Created new folder '{folder_name}': {folder_id}")
        return folder_id
        
    except HttpError as e:
        print(f"ERROR: Failed to get/create folder '{folder_name}': {str(e)}")
        raise
    except Exception as e:
        print(f"ERROR: Unexpected error in get_or_create_folder: {str(e)}")
        raise

def upload_file_to_folder(file_content, folder_id, filename, mime_type='application/pdf'):
    """
    Upload file to Google Drive folder
    
    Args:
        file_content: File content as bytes
        folder_id: ID of folder to upload to
        filename: Name for the file
        mime_type: MIME type of file (default: application/pdf)
        
    Returns:
        Dictionary with file_id and web_view_link
    """
    service = get_drive_service()
    
    try:
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaInMemoryUpload(
            file_content,
            mimetype=mime_type,
            resumable=True
        )
        
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink, webContentLink',
            supportsAllDrives=True
        ).execute()
        
        file_id = file.get('id')
        web_view_link = file.get('webViewLink')
        web_content_link = file.get('webContentLink')
        
        print(f"DEBUG Uploaded file '{filename}' to folder {folder_id}: {file_id}")
        
        # Make file accessible to anyone with the link
        try:
            permission = {
                'type': 'anyone',
                'role': 'reader'
            }
            service.permissions().create(
                fileId=file_id,
                body=permission,
                supportsAllDrives=True
            ).execute()
            print(f"DEBUG Made file publicly accessible: {file_id}")
        except Exception as e:
            print(f"WARNING: Could not set public permissions on file: {str(e)}")
        
        return {
            'file_id': file_id,
            'web_view_link': web_view_link,
            'web_content_link': web_content_link
        }
        
    except HttpError as e:
        print(f"ERROR: Failed to upload file '{filename}': {str(e)}")
        raise
    except Exception as e:
        print(f"ERROR: Unexpected error in upload_file_to_folder: {str(e)}")
        raise

def list_folder_contents(folder_id, page_size=100):
    """
    List all files in a folder
    
    Args:
        folder_id: ID of folder to list
        page_size: Number of results per page (default: 100)
        
    Returns:
        List of file dictionaries with id, name, mimeType
    """
    service = get_drive_service()
    
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        
        results = service.files().list(
            q=query,
            pageSize=page_size,
            fields='files(id, name, mimeType, createdTime)',
            orderBy='createdTime desc',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        print(f"DEBUG Found {len(files)} files in folder {folder_id}")
        
        return files
        
    except HttpError as e:
        print(f"ERROR: Failed to list folder contents: {str(e)}")
        raise
    except Exception as e:
        print(f"ERROR: Unexpected error in list_folder_contents: {str(e)}")
        raise

def delete_file(file_id):
    """
    Delete a file from Google Drive
    
    Args:
        file_id: ID of file to delete
    """
    service = get_drive_service()
    
    try:
        service.files().delete(
            fileId=file_id,
            supportsAllDrives=True
        ).execute()
        print(f"DEBUG Deleted file: {file_id}")
        
    except HttpError as e:
        print(f"ERROR: Failed to delete file {file_id}: {str(e)}")
        raise
    except Exception as e:
        print(f"ERROR: Unexpected error in delete_file: {str(e)}")
        raise

def create_shortcut(file_id, shortcut_name, parent_folder_id):
    """
    Create a shortcut to a file in Google Drive
    
    Args:
        file_id: ID of file to create shortcut to
        shortcut_name: Name for the shortcut
        parent_folder_id: ID of folder to place shortcut in
        
    Returns:
        Shortcut ID if created successfully, None otherwise
    """
    service = get_drive_service()
    
    try:
        shortcut_metadata = {
            'name': shortcut_name,
            'mimeType': 'application/vnd.google-apps.shortcut',
            'shortcutDetails': {
                'targetId': file_id
            },
            'parents': [parent_folder_id]
        }
        
        shortcut = service.files().create(
            body=shortcut_metadata,
            fields='id',
            supportsAllDrives=True
        ).execute()
        
        shortcut_id = shortcut.get('id')
        print(f"DEBUG Created shortcut '{shortcut_name}' in folder {parent_folder_id}: {shortcut_id}")
        return shortcut_id
        
    except HttpError as e:
        print(f"ERROR: Failed to create shortcut '{shortcut_name}': {str(e)}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error in create_shortcut: {str(e)}")
        return None

# Version: 1763228378
