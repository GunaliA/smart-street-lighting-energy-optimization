from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/historical/street_light_data.csv"


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("StreetLightingFaultDetection")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(HDFS_INPUT)
    )

    print("=" * 60)
    print("SMART STREET LIGHTING - FAULT DETECTION")
    print("=" * 60)

    print("\nFault Status Summary:")
    df.groupBy("fault_status").agg(
        count("*").alias("fault_count")
    ).orderBy("fault_status").show(truncate=False)

    print("\nDetailed Fault Records:")

    fault_df = df.filter(
        col("fault_status") != "NORMAL"
    )

    fault_df.select(
        "timestamp",
        "light_id",
        "location",
        "voltage",
        "current",
        "power_consumption",
        "temperature",
        "light_status",
        "fault_status"
    ).show(20, truncate=False)

    print(f"Total fault records: {fault_df.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
