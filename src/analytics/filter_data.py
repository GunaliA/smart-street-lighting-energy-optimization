from pyspark.sql import SparkSession


HDFS_INPUT = (
    "hdfs://localhost:9000/"
    "smart-street-lighting/historical/street_light_data.csv"
)

OUTPUT_PATH = "data/processed/filtered_data.csv"


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("StreetLightDataFiltering")
        .getOrCreate()
    )

    print("=" * 70)
    print("SMART STREET LIGHTING - FILTERED DATA ANALYSIS")
    print("=" * 70)

    print(f"Reading data from: {HDFS_INPUT}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(HDFS_INPUT)
    )

    print(f"Total records: {df.count():,}")

    # ------------------------------------------------------------
    # Available filter values
    # ------------------------------------------------------------

    print("\n" + "=" * 70)
    print("AVAILABLE FILTER VALUES")
    print("=" * 70)

    print("Locations:")
    df.select("location").distinct().orderBy("location").show(
        truncate=False
    )

    print("Traffic Levels:")
    df.select("traffic_level").distinct().orderBy("traffic_level").show(
        truncate=False
    )

    print("Light Status:")
    df.select("light_status").distinct().orderBy("light_status").show(
        truncate=False
    )

    print("Fault Status:")
    df.select("fault_status").distinct().orderBy("fault_status").show(
        truncate=False
    )

    # ------------------------------------------------------------
    # Example reusable filter
    #
    # Change these values later when connecting the dashboard.
    # Use None to ignore a filter.
    # ------------------------------------------------------------

    selected_location = None
    selected_traffic = None
    selected_light_status = None
    selected_fault_status = None

    filtered_df = df

    if selected_location is not None:
        filtered_df = filtered_df.filter(
            filtered_df.location == selected_location
        )

    if selected_traffic is not None:
        filtered_df = filtered_df.filter(
            filtered_df.traffic_level == selected_traffic
        )

    if selected_light_status is not None:
        filtered_df = filtered_df.filter(
            filtered_df.light_status == selected_light_status
        )

    if selected_fault_status is not None:
        filtered_df = filtered_df.filter(
            filtered_df.fault_status == selected_fault_status
        )

    # ------------------------------------------------------------
    # Filtered result
    # ------------------------------------------------------------

    print("\n" + "=" * 70)
    print("FILTERED DATA RESULT")
    print("=" * 70)

    print(f"Location filter: {selected_location}")
    print(f"Traffic filter: {selected_traffic}")
    print(f"Light status filter: {selected_light_status}")
    print(f"Fault status filter: {selected_fault_status}")

    filtered_count = filtered_df.count()

    print(f"\nFiltered records: {filtered_count:,}")

    filtered_df.orderBy(
        filtered_df.timestamp.desc()
    ).show(20, truncate=False)

    # ------------------------------------------------------------
    # Save filtered data
    # ------------------------------------------------------------

    print("\n" + "=" * 70)
    print("SAVING RESULT")
    print("=" * 70)

    (
        filtered_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    print(f"Filtered data saved to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()