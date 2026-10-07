import json
import subprocess
from collections import Counter


HDFS_DIR = "/smart-street-lighting/stream"


def hdfs_cat():
    """
    Read all JSON files stored in the HDFS stream directory.
    """

    result = subprocess.run(
        [
            "hdfs.cmd",
            "dfs",
            "-cat",
            f"{HDFS_DIR}/*.json",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"HDFS command failed:\n{result.stderr}"
        )

    return result.stdout.splitlines()


def main():

    print("=" * 60)
    print("KAFKA -> HDFS STREAM VALIDATION")
    print("=" * 60)

    lines = hdfs_cat()

    records = [
        json.loads(line)
        for line in lines
        if line.strip()
    ]

    # Use the same duplicate definition as the original
    # validator: timestamp + light_id + location +
    # power_consumption.
    keys = [
        (
            record.get("timestamp"),
            record.get("light_id"),
            record.get("location"),
            record.get("power_consumption"),
        )
        for record in records
    ]

    key_counts = Counter(keys)

    duplicate_keys = {
        key: count
        for key, count in key_counts.items()
        if count > 1
    }

    # Number of extra records caused by duplicate keys.
    duplicate_count = sum(
        count - 1
        for count in duplicate_keys.values()
    )

    print(
        f"HDFS JSON records : {len(records):,}"
    )

    print(
        f"Unique records    : {len(key_counts):,}"
    )

    print(
        f"Duplicate records : {duplicate_count:,}"
    )

    if records:

        print(
            f"First timestamp   : "
            f"{records[0].get('timestamp')}"
        )

        print(
            f"Last timestamp    : "
            f"{records[-1].get('timestamp')}"
        )

    if duplicate_keys:

        print()
        print("DUPLICATE RECORDS FOUND")
        print("-" * 60)

        for key, count in duplicate_keys.items():

            timestamp = key[0]
            light_id = key[1]
            location = key[2]
            power = key[3]

            print(
                f"Count: {count} | "
                f"timestamp={timestamp} | "
                f"light_id={light_id} | "
                f"location={location} | "
                f"power_consumption={power}"
            )

        print("-" * 60)

        raise SystemExit(
            "VALIDATION FAILED: duplicate records detected"
        )

    print()
    print("VALIDATION PASSED")


if __name__ == "__main__":
    main()