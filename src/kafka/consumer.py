import json
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "street-light-data"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Kafka consumer started.")
print(f"Reading topic: {KAFKA_TOPIC}")

count = 0

for message in consumer:
    count += 1

    if count <= 5:
        print(message.value)

    if count % 10_000 == 0:
        print(f"Consumed {count:,} records")
