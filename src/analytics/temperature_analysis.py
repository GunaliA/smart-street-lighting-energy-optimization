from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    min as spark_min,
    max as spark_max,
    count,
    when
)

HDFS_INPUT = (
    "hdfs://localhost:9000/"
    "smart-street-lighting/historical/street_light_data.csv"
)

OUTPUT_PATH = "data/processed/temperature_analysis.csv"


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TemperatureAnalysis")
        .getOrCreate()
    )

    print("=" * 70)
    print("SMART STREET LIGHTING - TEMPERATURE ANALYSIS")
    print("=" * 70)

    print(f"Reading data from: {HDFS_INPUT}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(HDFS_INPUT)
    )

    print(f"Total records: {df.count():,}")

    temperature_analysis = (
        df.withColumn(
            "temperature_range",
            when(df.temperature < 25, "Below 25°C")
            .when(df.temperature < 30, "25°C - 29.9°C")
            .when(df.temperature < 35, "30°C - 34.9°C")
            .otherwise("35°C and above")
        )
        .groupBy("temperature_range")
        .agg(
            count("*").alias("record_count"),
            avg("temperature").alias("average_temperature"),
            spark_min("temperature").alias("minimum_temperature"),
            spark_max("temperature").alias("maximum_temperature"),
            avg("power_consumption").alias("average_power")
        )
        .orderBy("temperature_range")
    )

    print("\n" + "=" * 70)
    print("TEMPERATURE ANALYSIS")
    print("=" * 70)

    temperature_analysis.show(truncate=False)

    print("\n" + "=" * 70)
    print("SAVING RESULT")
    print("=" * 70)

    (
        temperature_analysis
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