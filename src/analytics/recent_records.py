from pyspark.sql import SparkSession

HDFS_INPUT = (
    "hdfs://localhost:9000/"
    "smart-street-lighting/historical/street_light_data.csv"
)

OUTPUT_PATH = "data/processed/recent_records.csv"


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("RecentStreetLightRecords")
        .getOrCreate()
    )

    print("=" * 70)
    print("SMART STREET LIGHTING - RECENT RECORD ANALYSIS")
    print("=" * 70)

    print(f"Reading data from: {HDFS_INPUT}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(HDFS_INPUT)
    )

    print(f"Total records: {df.count():,}")

    # Get the most recent records based on timestamp
    recent_records = (
        df.orderBy(df.timestamp.desc())
        .limit(20)
    )

    print("\n" + "=" * 70)
    print("20 MOST RECENT STREET LIGHT RECORDS")
    print("=" * 70)

    recent_records.select(
        "timestamp",
        "light_id",
        "location",
        "traffic_level",
        "motion_detected",
        "brightness_level",
        "power_consumption",
        "temperature",
        "light_status",
        "fault_status"
    ).show(20, truncate=False)

    print("\n" + "=" * 70)
    print("SAVING RESULT")
    print("=" * 70)

    (
        recent_records
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    print(f"Result saved to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()