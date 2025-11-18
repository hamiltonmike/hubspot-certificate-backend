import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive']

creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
creds_dict = json.loads(creds_json)
credentials = service_account.Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
service = build('drive', 'v3', credentials=credentials)

# Try to access the _MASTER_CERTIFICATES folder
folder_id = '1fZZEV_l4gDV9AbjRD0k-7zvZAqxIT2ft'

try:
    result = service.files().get(
        fileId=folder_id,
        fields='id, name, parents',
        supportsAllDrives=True
    ).execute()
    print(f"SUCCESS! Can access folder: {result}")
except Exception as e:
    print(f"ERROR: Cannot access folder: {e}")

# List shared drives
try:
    drives = service.drives().list().execute()
    print(f"\nShared Drives visible to service account:")
    for drive in drives.get('drives', []):
        print(f"  - {drive['name']} (ID: {drive['id']})")
except Exception as e:
    print(f"ERROR listing drives: {e}")
