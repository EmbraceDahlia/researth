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
    "parameters_id": 2,
    "limit": LIMIT
}

print(f"Streaming from {API_URL} every {POLL_INTERVAL}s to topic {TOPIC}")

while True:
    try:
        response = requests.get(
            API_URL,
            headers=headers,
            params=params
        )

        response.raise_for_status()
        data = response.json()

        # debugging
        # print(json.dumps(data, indent=2))

        results = data.get("results", [])
        for item in results:

            location = item.get("name")
            country = item.get("country", {}).get("name")
            coords = item.get("coordinates", {})
            sensors = item.get("sensors", [])
            
            for sensor in sensors:

                parameter = sensor.get("parameter", {})
                # Keep only PM2.5
                if parameter.get("id") != 2:
                    continue
                timestamp = (item.get("datetimeLast") or {}).get("utc")
                message = {
                    "location": location,
                    "country": country,
                    "latitude": coords.get("latitude"),
                    "longitude": coords.get("longitude"),
                    "parameter_id": parameter.get("id"),
                    "parameter": parameter.get("name"),
                    "unit": parameter.get("units"),
                    "sensor_id": sensor.get("id"),
                    "timestamp": timestamp
                }

                producer.send(TOPIC, value=message)
                print("Sent:", message)

        time.sleep(POLL_INTERVAL)

    except Exception as e:
        print("Error:", e)
        time.sleep(5)