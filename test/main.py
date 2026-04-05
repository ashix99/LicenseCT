import time
import requests

BASE_URL = "https://receipt-api.nitro.xin"

cdk = input("Enter CDK: ").strip()
session_value = input("Enter session value: ").strip()

resp = requests.post(
    f"{BASE_URL}/stocks/public/outstock",
    json={"cdk": cdk, "user": session_value},
    timeout=30,
)
resp.raise_for_status()

task_id = resp.text.strip()
print("Task ID:", task_id)

while True:
    r = requests.get(f"{BASE_URL}/stocks/public/outstock/{task_id}", timeout=30)
    r.raise_for_status()
    data = r.json()
    print(data)

    if data.get("pending") is False:
        break

    time.sleep(10)