import requests
import json

url = "https://api.zigment.ai/reporting/preview"

headers = {
    "Cache-Control": "no-cache",
    "Postman-Token": "generate_unique_token",  # You can generate a random UUID
    "Content-Type": "application/json",
    "User-Agent": "PostmanRuntime/7.48.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "x-org-id": "6617aafc195dea3f1dbdd894",
    "zigment-x-api-key": "sk_5b1960bfac64b7e2c7f91f2383a11ff3"
}

# Payload for the POST request
payload = {
    "sqlText": "SELECT event_id, category, type, timestamp, channel FROM events",
    "type": "table"
}

# Make POST request
response = requests.post(url, headers=headers, json=payload)

# Print response
print(f"Status Code: {response.status_code}")
print(f"Response Headers: {dict(response.headers)}")
print(f"\nResponse Body:")
print(json.dumps(response.json(), indent=2) if response.headers.get('content-type') == 'application/json' else response.text)

