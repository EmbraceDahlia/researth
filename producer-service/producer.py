import os
import json
import time
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = os.getenv("KAFKA_TOPIC")

API_URL = os.getenv("OPENAQ_API_URL")
API_KEY = os.getenv("OPENAQ_API_KEY")

LIMIT = int(os.getenv("LIMIT", 100))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

headers = {
    "X-API-Key": API_KEY
}

params = {
    "limit": LIMIT
}

print(f"Streaming from {API_URL} every {POLL_INTERVAL}s to topic {TOPIC}")

while True:
    try:
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])

        for item in results:
            message = {
                "location": item.get("name"),
                "country": item.get("country", {}).get("name"),
                "coordinates": item.get("coordinates")
            }

            producer.send(TOPIC, value=message)
            print("Sent:", message)

        time.sleep(POLL_INTERVAL)

    except Exception as e:
        print("Error:", e)
        time.sleep(5)