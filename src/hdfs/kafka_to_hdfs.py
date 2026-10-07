import json
import os
import subprocess
import tempfile
import time
from collections import defaultdict

from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata


KAFKA_TOPIC = "street-light-data"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_GROUP = "hdfs-sink-v3"

HDFS_DIR = "/smart-street-lighting/stream"

BATCH_SIZE = 1000
FLUSH_INTERVAL = 10

HDFS_CMD = r"C:\hadoop-3.3.1\bin\hdfs.cmd"


def run_hdfs(arguments):
    """
    Run HDFS through Windows cmd.exe and return the completed process.
    """

    command = [
        "cmd.exe",
        "/c",
        HDFS_CMD,
        "dfs",
    ] + arguments

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "\nHDFS COMMAND FAILED\n"
            f"Command: {command}\n"
            f"Return code: {result.returncode}\n"
            f"STDOUT: {result.stdout}\n"
            f"STDERR: {result.stderr}\n"
        )

    return result


def ensure_hdfs_directory():
    """
    Create the HDFS output directory if it does not exist.
    """

    result = run_hdfs(
        [
            "-mkdir",
            "-p",
            HDFS_DIR,
        ]
    )

    print("HDFS directory ready:")
    print(HDFS_DIR)

    if result.stdout.strip():
        print(result.stdout.strip())


def batch_path(partition, first_offset, last_offset):
    """
    Generate a deterministic HDFS filename for a Kafka batch.
    """

    return (
        f"{HDFS_DIR}/partition-{partition:05d}-"
        f"{first_offset:020d}-{last_offset:020d}.json"
    )


def hdfs_file_exists(path):
    """
    Check whether an exact HDFS file exists.

    IMPORTANT:
    On this Windows Hadoop setup, `hdfs dfs -test -e`
    incorrectly returns exit code 0 even when the file
    does not exist.

    Therefore, use `hdfs dfs -ls` and inspect the actual
    returned listing instead of relying on the exit code.
    """

    result = run_hdfs(
        [
            "-ls",
            path,
        ]
    )

    output = (result.stdout or "").strip()

    if not output:
        return False

    for line in output.splitlines():

        line = line.strip()

        if line.endswith(path):
            return True

    return False


def write_batch(partition, records):
    """
    Write one Kafka batch to HDFS.
    """

    if not records:
        return None

    records = sorted(
        records,
        key=lambda item: item["offset"],
    )

    first_offset = records[0]["offset"]
    last_offset = records[-1]["offset"]

    target = batch_path(
        partition,
        first_offset,
        last_offset,
    )

    print()
    print("-" * 60)
    print("WRITING HDFS BATCH")
    print(f"Partition : {partition}")
    print(f"Records   : {len(records):,}")
    print(f"Offsets   : {first_offset}-{last_offset}")
    print(f"HDFS file : {target}")

    # Check whether this exact batch was already written.
    if hdfs_file_exists(target):

        print("HDFS file already exists. Skipping upload.")
        print("-" * 60)

        return last_offset

    temp_path = None

    try:

        # --------------------------------------------------
        # Create local JSON batch
        # --------------------------------------------------

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".json",
            delete=False,
        ) as temp_file:

            temp_path = temp_file.name

            for item in records:

                temp_file.write(
                    json.dumps(
                        item["value"],
                        ensure_ascii=False,
                    )
                )

                temp_file.write("\n")

        local_size = os.path.getsize(
            temp_path
        )

        print(f"Local file : {temp_path}")
        print(f"Local size : {local_size:,} bytes")

        if local_size == 0:
            raise RuntimeError(
                "Generated local batch file is empty."
            )

        # --------------------------------------------------
        # Upload local batch to HDFS
        # --------------------------------------------------

        result = run_hdfs(
            [
                "-put",
                "-f",
                temp_path,
                target,
            ]
        )

        print("HDFS PUT completed.")

        if result.stdout.strip():

            print("HDFS STDOUT:")
            print(result.stdout.strip())

        if result.stderr.strip():

            print("HDFS STDERR:")
            print(result.stderr.strip())

        # --------------------------------------------------
        # Verify the actual HDFS file
        # --------------------------------------------------

        if not hdfs_file_exists(target):

            raise RuntimeError(
                "HDFS PUT returned success, but the target "
                "file cannot be found in HDFS:\n"
                f"{target}"
            )

        print("HDFS verification: PASSED")
        print(
            f"Transferred {len(records):,} records"
        )
        print("-" * 60)

        return last_offset

    finally:

        if (
            temp_path
            and os.path.exists(temp_path)
        ):

            os.remove(temp_path)


def process_batches(consumer, pending):
    """
    Write pending batches and commit Kafka offsets only
    after successful HDFS processing.
    """

    offsets_to_commit = {}

    for partition in sorted(pending):

        records = pending[partition]

        if not records:
            continue

        for start in range(
            0,
            len(records),
            BATCH_SIZE,
        ):

            batch = records[
                start:start + BATCH_SIZE
            ]

            last_offset = write_batch(
                partition,
                batch,
            )

            offsets_to_commit[
                TopicPartition(
                    KAFKA_TOPIC,
                    partition,
                )
            ] = OffsetAndMetadata(
                last_offset + 1,
                None,
            )

    if offsets_to_commit:

        consumer.commit(
            offsets=offsets_to_commit
        )

        print(
            "Kafka offsets committed."
        )

    pending.clear()


def main():

    print("=" * 60)
    print("KAFKA -> HDFS STREAMING INGESTION")
    print("=" * 60)

    print(
        f"Kafka topic    : {KAFKA_TOPIC}"
    )

    print(
        f"Consumer group : {KAFKA_GROUP}"
    )

    print(
        f"HDFS output    : {HDFS_DIR}"
    )

    print(
        f"Batch size     : {BATCH_SIZE}"
    )

    print(
        f"HDFS command   : {HDFS_CMD}"
    )

    print(
        "Verification   : ENABLED"
    )

    print(
        "Commit policy  : "
        "HDFS success BEFORE Kafka commit"
    )

    print("=" * 60)

    ensure_hdfs_directory()

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    consumer.subscribe(
        [KAFKA_TOPIC]
    )

    pending = defaultdict(list)

    last_flush = time.time()

    print(
        "Kafka consumer started."
    )

    print(
        "Waiting for records..."
    )

    try:

        while True:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=5000,
            )

            for _tp, messages in records.items():

                for message in messages:

                    value = json.loads(
                        message.value.decode(
                            "utf-8"
                        )
                    )

                    pending[
                        message.partition
                    ].append(
                        {
                            "offset": message.offset,
                            "value": value,
                        }
                    )

            total_pending = sum(
                len(items)
                for items in pending.values()
            )

            if (
                total_pending >= BATCH_SIZE
                or (
                    total_pending
                    and (
                        time.time()
                        - last_flush
                        >= FLUSH_INTERVAL
                    )
                )
            ):

                process_batches(
                    consumer,
                    pending,
                )

                last_flush = time.time()

            if (
                not records
                and not pending
            ):

                time.sleep(0.25)

    except KeyboardInterrupt:

        print()
        print(
            "Stopping Kafka -> HDFS ingestion..."
        )

        if pending:

            process_batches(
                consumer,
                pending,
            )

    finally:

        consumer.close()

        print(
            "Kafka consumer closed."
        )


if __name__ == "__main__":
    main()