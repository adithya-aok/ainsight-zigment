import requests

url = "https://api.zigment.ai/schemas/schemaForAllowedCollections"


headers = {
    "Cache-Control": "no-cache",
    "Postman-Token": "generate_unique_token",  # You can generate a random UUID
    "User-Agent": "PostmanRuntime/7.48.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "x-org-id": "6617aafc195dea3f1dbdd894",
    "zigment-x-api-key": os.getenv("ZIGMENT_API_KEY", "")
}

# For GET request
response = requests.get(url, headers=headers)

# For POST request (if needed)
# response = requests.post(url, headers=headers, json=your_data)

print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")
