import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

NUM_LIGHTS = 10_000
NUM_RECORDS = 100_000
SEED = 42
SAMPLE_INTERVAL_SECONDS = 1
OUTPUT_DIR = Path("data/sample")
OUTPUT_FILE = OUTPUT_DIR / "street_light_data.csv"

LOCATIONS = [f"Zone_{i:02d}" for i in range(1, 11)]
ZONE_COORDINATES = {
    "Zone_01": (13.0827, 80.2707), "Zone_02": (13.0674, 80.2376),
    "Zone_03": (13.0475, 80.2090), "Zone_04": (13.0604, 80.2496),
    "Zone_05": (13.0878, 80.2785), "Zone_06": (13.0418, 80.2341),
    "Zone_07": (13.1143, 80.2915), "Zone_08": (13.0108, 80.2167),
    "Zone_09": (13.0339, 80.2619), "Zone_10": (13.1037, 80.2275),
}

FIELDNAMES = [
    "timestamp", "light_id", "location", "latitude", "longitude",
    "ldr_value", "motion_detected", "traffic_level", "brightness_level",
    "voltage", "current", "power_consumption", "temperature",
    "light_status", "fault_status",
]


def generate_light_id(light_number):
    return f"SL{light_number:05d}"


def build_light_catalog():
    """Create a fixed physical inventory; each light permanently belongs to one zone."""
    rng = random.Random(SEED)
    catalog = {}
    lights_per_zone = NUM_LIGHTS // len(LOCATIONS)
    for number in range(1, NUM_LIGHTS + 1):
        location = LOCATIONS[(number - 1) // lights_per_zone]
        base_lat, base_lon = ZONE_COORDINATES[location]
        local_rng = random.Random(rng.randint(0, 10**9))
        catalog[generate_light_id(number)] = {
            "location": location,
            "latitude": round(base_lat + local_rng.uniform(-0.002, 0.002), 6),
            "longitude": round(base_lon + local_rng.uniform(-0.002, 0.002), 6),
        }
    return catalog


def generate_timestamp(record_number):
    start = datetime(2026, 9, 29, 0, 0, 0)
    return (start + timedelta(seconds=record_number * SAMPLE_INTERVAL_SECONDS)).strftime("%Y-%m-%dT%H:%M:%S")


def generate_traffic_level(rng):
    return rng.choices(["LOW", "MEDIUM", "HIGH"], weights=[30, 45, 25], k=1)[0]


def generate_motion(rng):
    return rng.choices([0, 1], weights=[35, 65], k=1)[0]


def calculate_brightness(traffic_level, motion_detected):
    if traffic_level == "HIGH":
        brightness = 100
    elif traffic_level == "MEDIUM":
        brightness = 75
    else:
        brightness = 40
    if motion_detected:
        brightness += 10
    return min(brightness, 100)


def calculate_power(brightness, voltage, current):
    return round(voltage * current * (brightness / 100), 2)


def generate_fault(rng):
    return rng.choices(
        ["NORMAL", "LOW_VOLTAGE", "HIGH_CURRENT", "LIGHT_FAILURE"],
        weights=[97, 1, 1, 1], k=1
    )[0]


def generate_record(record_number, catalog, rng, forced_light_id=None):
    light_id = forced_light_id or generate_light_id(rng.randint(1, NUM_LIGHTS))
    physical = catalog[light_id]
    traffic = generate_traffic_level(rng)
    motion = generate_motion(rng)
    brightness = calculate_brightness(traffic, motion)
    voltage = round(rng.uniform(220.0, 240.0), 2)
    current = round(rng.uniform(0.30, 1.20), 2)
    fault = generate_fault(rng)
    temperature = round(rng.uniform(24.0, 38.0), 1)
    ldr = rng.randint(50, 900)

    if fault == "LOW_VOLTAGE":
        voltage = round(rng.uniform(180.0, 210.0), 2)
    elif fault == "HIGH_CURRENT":
        current = round(rng.uniform(1.50, 2.50), 2)
    elif fault == "LIGHT_FAILURE":
        brightness = 0

    status = "FAULT" if fault == "LIGHT_FAILURE" else ("ON" if brightness > 0 else "OFF")

    return {
        "timestamp": generate_timestamp(record_number),
        "light_id": light_id,
        **physical,
        "ldr_value": ldr,
        "motion_detected": motion,
        "traffic_level": traffic,
        "brightness_level": brightness,
        "voltage": voltage,
        "current": current,
        "power_consumption": calculate_power(brightness, voltage, current),
        "temperature": temperature,
        "light_status": status,
        "fault_status": fault,
    }


def generate_dataset():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    catalog = build_light_catalog()
    rng = random.Random(SEED)
    print("SMART STREET LIGHTING DATASET SIMULATOR")
    print(f"Virtual street lights : {NUM_LIGHTS:,}")
    print(f"Records to generate   : {NUM_RECORDS:,}")
    print(f"Sample interval       : {SAMPLE_INTERVAL_SECONDS} second")
    print(f"Output file           : {OUTPUT_FILE}")

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        for i in range(NUM_RECORDS):
            # Guarantee every physical light appears at least once.
            forced = generate_light_id(i + 1) if i < NUM_LIGHTS else None
            writer.writerow(generate_record(i, catalog, rng, forced))
            if (i + 1) % 10_000 == 0:
                print(f"Generated {i + 1:,} / {NUM_RECORDS:,} records")

    print("DATASET GENERATION COMPLETED")


if __name__ == "__main__":
    generate_dataset()
