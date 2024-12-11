import requests
base_url = "http://127.0.0.1:5000"
# Test cases
endpoints = [
    {"url": f"{base_url}/getNOrders", "tokens": {'limit':20}},
    {"url": f"{base_url}/getNCustomers", "tokens": {'limit':20}},
    {"url": f"{base_url}/getNSellers", "tokens": {'limit':20}},
    {"url": f"{base_url}/getOrders", "tokens": {"start": "2016-01-01", "end": "2017-12-31"}},
    {"url": f"{base_url}/getNProducts", "tokens": {'limit':20}},
    {"url": f"{base_url}/getLocationsWithHighestAvgOrderValue", "tokens": {'limit': 10}},
    {"url": f"{base_url}/getMostFrequentProductCategories", "tokens": {'limit': 10}},
    {"url": f"{base_url}/getMostFrequentPurchaseHours", "tokens": {'limit': 5}},
    {"url": f"{base_url}/getMostProfitableLocations", "tokens": {'limit': 10}},
    {"url": f"{base_url}/getTop5CustomersOnSpendings", "tokens": {}}
]
for endpoint in endpoints:
    try:
        print(f"Testing endpoint: {endpoint['url']} with parameters: {endpoint['tokens']}")
        response = requests.get(endpoint["url"], params=endpoint["tokens"])
        print(f"Request: {endpoint['url']}")
        print("Response:", response.json(), "\n")
    except Exception as e:
        print(f"Error making request to {endpoint['url']}: {e}")