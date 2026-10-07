from pyspark.sql import SparkSession


HDFS_INPUT = (
    "hdfs://localhost:9000/"
    "smart-street-lighting/historical/street_light_data.csv"
)

OUTPUT_PATH = "data/processed/exported_filtered_data.csv"


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("FilteredDataExport")
        .getOrCreate()
    )

    print("=" * 70)
    print("SMART STREET LIGHTING - FILTERED DATA EXPORT")
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
    # Filter selections
    #
    # Use None to ignore a filter.
    # These values will later be supplied by the dashboard.
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
    # Export
    # ------------------------------------------------------------

    exported_count = filtered_df.count()

    print("\n" + "=" * 70)
    print("EXPORT SUMMARY")
    print("=" * 70)

    print(f"Location filter: {selected_location}")
    print(f"Traffic filter: {selected_traffic}")
    print(f"Light status filter: {selected_light_status}")
    print(f"Fault status filter: {selected_fault_status}")
    print(f"Records to export: {exported_count:,}")

    (
        filtered_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(OUTPUT_PATH)
    )

    print(f"\nFiltered dataset exported to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()