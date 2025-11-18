import hmac
import hashlib
import base64
import time
import sys

if len(sys.argv) < 2:
    print("Usage: python3 test_signature.py YOUR_CLIENT_SECRET")
    sys.exit(1)

CLIENT_SECRET = sys.argv[1]
METHOD = "POST"
URL = "https://hubspot-certificate-backend-486092186709.us-central1.run.app/api/generate-certificate"
BODY = '{"certificateData":{"siteName":"Test","requestorName":"John","brokerCompany":"ABC","brokerContact":"Jane","brokerEmail":"test@test.com","certificateDate":"Nov 12"}}'
TIMESTAMP = str(int(time.time() * 1000))

source_string = f"{METHOD}{URL}{BODY}{TIMESTAMP}"
signature_bytes = hmac.new(CLIENT_SECRET.encode('utf-8'), source_string.encode('utf-8'), hashlib.sha256).digest()
signature = base64.b64encode(signature_bytes).decode('utf-8')

print(f"curl -X POST {URL} \\")
print(f'  -H "Content-Type: application/json" \\')
print(f'  -H "X-HubSpot-Signature-v3: {signature}" \\')
print(f'  -H "X-HubSpot-Request-Timestamp: {TIMESTAMP}" \\')
print(f"  -d '{BODY}'")
