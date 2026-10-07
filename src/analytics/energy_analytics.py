from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, count, max, min

HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/historical/street_light_data.csv"
SAMPLE_INTERVAL_SECONDS = 1


def with_energy(df):
    return df.withColumn(
        "energy_wh",
        df.power_consumption * SAMPLE_INTERVAL_SECONDS / 3600.0
    )


def main():
    spark = (SparkSession.builder.master("local[*]").appName("StreetLightingEnergyAnalytics").getOrCreate())
    df = with_energy(spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_INPUT))
    print("=" * 60)
    print("SMART STREET LIGHTING - ENERGY ANALYTICS")
    print("=" * 60)
    print(f"Sample interval: {SAMPLE_INTERVAL_SECONDS} second")
    df.select(
        count("*").alias("record_count"),
        avg("power_consumption").alias("average_power_w"),
        spark_sum("energy_wh").alias("total_energy_wh"),
        (spark_sum("energy_wh") / 1000.0).alias("total_energy_kwh"),
        max("power_consumption").alias("maximum_power_w"),
        min("power_consumption").alias("minimum_power_w")
    ).show(truncate=False)
    print("\nEnergy by Traffic Level:")
    df.groupBy("traffic_level").agg(
        count("*").alias("record_count"),
        spark_sum("energy_wh").alias("total_energy_wh"),
        (spark_sum("energy_wh") / 1000.0).alias("total_energy_kwh"),
        avg("power_consumption").alias("average_power_w")
    ).orderBy("traffic_level").show(truncate=False)
    print("\nEnergy by Zone:")
    df.groupBy("location").agg(
        count("*").alias("record_count"),
        spark_sum("energy_wh").alias("total_energy_wh"),
        (spark_sum("energy_wh") / 1000.0).alias("total_energy_kwh"),
        avg("power_consumption").alias("average_power_w")
    ).orderBy("location").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
