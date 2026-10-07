import csv
import json
from pathlib import Path

from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "street-light-data"

DATA_FILE = Path("data/sample/street_light_data.csv")


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# SEND DATA TO KAFKA
# ============================================================

def send_data_to_kafka():

    print("=" * 60)
    print("SMART STREET LIGHTING - KAFKA PRODUCER")
    print("=" * 60)

    print(f"Reading data from : {DATA_FILE}")
    print(f"Kafka server      : {KAFKA_SERVER}")
    print(f"Kafka topic       : {KAFKA_TOPIC}")
    print()

    with open(
        DATA_FILE,
        mode="r",
        encoding="utf-8"
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        for count, row in enumerate(reader, start=1):

            producer.send(
                KAFKA_TOPIC,
                value=row
            )

            if count % 10_000 == 0:
                producer.flush()
                print(
                    f"Sent {count:,} records to Kafka"
                )

    producer.flush()

    print()
    print("=" * 60)
    print("KAFKA DATA INGESTION COMPLETED")
    print("=" * 60)


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":
    send_data_to_kafka()