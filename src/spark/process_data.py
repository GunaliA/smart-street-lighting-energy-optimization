from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
    round as spark_round,
    to_timestamp,
    row_number
)
from pyspark.sql.window import Window


HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/stream"


def load_data(spark):
    """
    Load Kafka -> HDFS JSON data and convert fields
    from JSON strings to proper Spark data types.
    """

    df = spark.read.json(HDFS_INPUT)

    df = (
        df
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
        .withColumn("ldr_value", col("ldr_value").cast("integer"))
        .withColumn("motion_detected", col("motion_detected").cast("integer"))
        .withColumn("brightness_level", col("brightness_level").cast("integer"))
        .withColumn("voltage", col("voltage").cast("double"))
        .withColumn("current", col("current").cast("double"))
        .withColumn("power_consumption", col("power_consumption").cast("double"))
        .withColumn("temperature", col("temperature").cast("double"))
    )

    # Every record represents one second.
    # Energy (Wh) = Power (W) / 3600
    df = df.withColumn(
        "energy_wh",
        col("power_consumption") / 3600.0
    )

    return df


def energy_analysis(df):
    print("\n" + "=" * 60)
    print("1. ENERGY CONSUMPTION ANALYSIS")
    print("=" * 60)

    result = df.agg(
        avg("power_consumption").alias("average_power"),
        spark_sum("energy_wh").alias("total_energy_wh"),
        spark_max("power_consumption").alias("maximum_power"),
        spark_min("power_consumption").alias("minimum_power")
    ).collect()[0]

    total_energy_wh = result["total_energy_wh"]
    total_energy_kwh = total_energy_wh / 1000.0

    print(f"Average Power : {result['average_power']:.2f} W")
    print(f"Total Energy  : {total_energy_wh:.2f} Wh")
    print(f"Total Energy  : {total_energy_kwh:.4f} kWh")
    print(f"Maximum Power : {result['maximum_power']:.2f} W")
    print(f"Minimum Power : {result['minimum_power']:.2f} W")


def motion_power_analysis(df):
    print("\n" + "=" * 60)
    print("2. MOTION VS POWER ANALYSIS")
    print("=" * 60)

    result = (
        df.groupBy("motion_detected")
        .agg(
            count("*").alias("records"),
            avg("power_consumption").alias("average_power"),
            spark_sum("energy_wh").alias("energy_wh")
        )
        .withColumn(
            "energy_kwh",
            col("energy_wh") / 1000.0
        )
        .orderBy("motion_detected")
    )

    result.select(
        "motion_detected",
        "records",
        spark_round("average_power", 2).alias("average_power_w"),
        spark_round("energy_kwh", 4).alias("energy_kwh")
    ).show(truncate=False)


def traffic_analysis(df):
    print("\n" + "=" * 60)
    print("3. TRAFFIC VS POWER AND ENERGY")
    print("=" * 60)

    result = (
        df.groupBy("traffic_level")
        .agg(
            count("*").alias("records"),
            avg("power_consumption").alias("average_power"),
            spark_sum("energy_wh").alias("energy_wh")
        )
        .withColumn(
            "energy_kwh",
            col("energy_wh") / 1000.0
        )
        .orderBy("traffic_level")
    )

    result.select(
        "traffic_level",
        "records",
        spark_round("average_power", 2).alias("average_power_w"),
        spark_round("energy_kwh", 4).alias("energy_kwh")
    ).show(truncate=False)


def brightness_analysis(df):
    print("\n" + "=" * 60)
    print("4. BRIGHTNESS ANALYSIS")
    print("=" * 60)

    result = (
        df.groupBy("brightness_level")
        .agg(
            count("*").alias("records"),
            avg("power_consumption").alias("average_power"),
            spark_sum("energy_wh").alias("energy_wh")
        )
        .withColumn(
            "energy_kwh",
            col("energy_wh") / 1000.0
        )
        .orderBy("brightness_level")
    )

    result.select(
        "brightness_level",
        "records",
        spark_round("average_power", 2).alias("average_power_w"),
        spark_round("energy_kwh", 4).alias("energy_kwh")
    ).show(truncate=False)


def ldr_analysis(df):
    print("\n" + "=" * 60)
    print("5. LDR / LIGHT LEVEL ANALYSIS")
    print("=" * 60)

    result = df.agg(
        avg("ldr_value").alias("average_ldr"),
        spark_min("ldr_value").alias("minimum_ldr"),
        spark_max("ldr_value").alias("maximum_ldr")
    ).collect()[0]

    print(f"Average LDR : {result['average_ldr']:.2f}")
    print(f"Minimum LDR : {result['minimum_ldr']}")
    print(f"Maximum LDR : {result['maximum_ldr']}")

    print("\nLDR range vs average power:")

    ldr_groups = (
        df.withColumn(
            "ldr_range",
            when(col("ldr_value") < 300, "LOW")
            .when(col("ldr_value") < 700, "MEDIUM")
            .otherwise("HIGH")
        )
        .groupBy("ldr_range")
        .agg(
            count("*").alias("records"),
            avg("power_consumption").alias("average_power"),
            spark_sum("energy_wh").alias("energy_wh")
        )
        .withColumn(
            "energy_kwh",
            col("energy_wh") / 1000.0
        )
        .orderBy("ldr_range")
    )

    ldr_groups.select(
        "ldr_range",
        "records",
        spark_round("average_power", 2).alias("average_power_w"),
        spark_round("energy_kwh", 4).alias("energy_kwh")
    ).show(truncate=False)


def temperature_analysis(df):
    print("\n" + "=" * 60)
    print("6. TEMPERATURE ANALYSIS")
    print("=" * 60)

    result = df.agg(
        avg("temperature").alias("average_temperature"),
        spark_min("temperature").alias("minimum_temperature"),
        spark_max("temperature").alias("maximum_temperature")
    ).collect()[0]

    print(f"Average Temperature : {result['average_temperature']:.2f} °C")
    print(f"Minimum Temperature : {result['minimum_temperature']:.2f} °C")
    print(f"Maximum Temperature : {result['maximum_temperature']:.2f} °C")

    print("\nTemperature range vs average power:")

    temperature_groups = (
        df.withColumn(
            "temperature_range",
            when(col("temperature") < 25, "LOW")
            .when(col("temperature") < 35, "MEDIUM")
            .otherwise("HIGH")
        )
        .groupBy("temperature_range")
        .agg(
            count("*").alias("records"),
            avg("power_consumption").alias("average_power")
        )
        .orderBy("temperature_range")
    )

    temperature_groups.select(
        "temperature_range",
        "records",
        spark_round("average_power", 2).alias("average_power_w")
    ).show(truncate=False)


def location_energy_analysis(df):
    print("\n" + "=" * 60)
    print("7. LOCATION / ZONE ENERGY ANALYSIS")
    print("=" * 60)

    result = (
        df.groupBy("location")
        .agg(
            count("*").alias("records"),
            avg("power_consumption").alias("average_power"),
            spark_sum("energy_wh").alias("energy_wh")
        )
        .withColumn(
            "energy_kwh",
            col("energy_wh") / 1000.0
        )
        .orderBy("location")
    )

    result.select(
        "location",
        "records",
        spark_round("average_power", 2).alias("average_power_w"),
        spark_round("energy_kwh", 4).alias("energy_kwh")
    ).show(20, truncate=False)


def power_over_time(df):
    print("\n" + "=" * 60)
    print("8. POWER OVER TIME")
    print("=" * 60)

    result = (
        df.groupBy("timestamp")
        .agg(
            avg("power_consumption").alias("average_power")
        )
        .orderBy("timestamp")
    )

    print("First 10 time points:")

    result.select(
        "timestamp",
        spark_round("average_power", 2).alias("average_power_w")
    ).show(10, truncate=False)


def fault_detection(df):
    print("\n" + "=" * 60)
    print("9. FAULT DETECTION")
    print("=" * 60)

    result = (
        df.groupBy("fault_status")
        .agg(
            count("*").alias("records")
        )
        .orderBy("fault_status")
    )

    result.show(truncate=False)

    faulty = df.filter(col("fault_status") != "NORMAL")

    print(f"Total Fault Records: {faulty.count():,}")


def system_health(df):
    print("\n" + "=" * 60)
    print("10. SYSTEM HEALTH")
    print("=" * 60)

    total_lights = df.select("light_id").distinct().count()

    # Get the latest record for EACH light.
    # This is important because every light has its own timestamp.
    window = Window.partitionBy("light_id").orderBy(
        col("timestamp").desc()
    )

    latest_per_light = (
        df
        .withColumn("row_number", row_number().over(window))
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    active_lights = (
        latest_per_light
        .filter(col("light_status") == "ON")
        .select("light_id")
        .distinct()
        .count()
    )

    faulty_lights = (
        latest_per_light
        .filter(col("fault_status") != "NORMAL")
        .select("light_id")
        .distinct()
        .count()
    )

    latest_timestamp = df.agg(
        spark_max("timestamp").alias("latest_timestamp")
    ).collect()[0]["latest_timestamp"]

    print(f"Total Lights   : {total_lights:,}")
    print(f"Latest Time    : {latest_timestamp}")
    print(f"Active Lights  : {active_lights:,}")
    print(f"Faulty Lights  : {faulty_lights:,}")


def energy_optimization(df):
    print("\n" + "=" * 60)
    print("11. ENERGY OPTIMIZATION")
    print("=" * 60)

    # Target brightness according to the optimization rules.
    optimized_brightness = (
        when(col("fault_status") == "LIGHT_FAILURE", 0)
        .when(col("ldr_value") >= 700, 30)
        .when(
            (col("traffic_level") == "LOW") &
            (col("motion_detected") == 0),
            40
        )
        .when(
            (col("traffic_level") == "LOW") &
            (col("motion_detected") == 1),
            50
        )
        .when(
            (col("traffic_level") == "MEDIUM") &
            (col("motion_detected") == 0),
            60
        )
        .when(
            (col("traffic_level") == "MEDIUM") &
            (col("motion_detected") == 1),
            75
        )
        .when(
            (col("traffic_level") == "HIGH") &
            (col("motion_detected") == 0),
            85
        )
        .otherwise(100)
    )

    optimized_df = df.withColumn(
        "optimized_brightness",
        optimized_brightness
    )

    # Power is scaled relative to the light's CURRENT brightness.
    #
    # Example:
    # Current brightness = 100%
    # Target brightness  = 50%
    # Optimized power    = current power * 50 / 100
    #
    # If current brightness is 50% and target is 75%:
    # optimized power = current power * 75 / 50
    #
    # A zero-brightness record is handled separately.
    optimized_df = optimized_df.withColumn(
        "optimized_power",
        when(
            col("brightness_level") <= 0,
            when(
                col("optimized_brightness") <= 0,
                0.0
            ).otherwise(col("power_consumption"))
        )
        .otherwise(
            col("power_consumption") *
            col("optimized_brightness") /
            col("brightness_level")
        )
    )

    optimized_df = optimized_df.withColumn(
        "optimized_energy_wh",
        col("optimized_power") / 3600.0
    )

    result = optimized_df.agg(
        spark_sum("energy_wh").alias("current_energy_wh"),
        spark_sum("optimized_energy_wh").alias("optimized_energy_wh")
    ).collect()[0]

    current_energy_kwh = result["current_energy_wh"] / 1000.0
    optimized_energy_kwh = result["optimized_energy_wh"] / 1000.0

    savings_kwh = current_energy_kwh - optimized_energy_kwh

    savings_percentage = (
        savings_kwh / current_energy_kwh * 100
        if current_energy_kwh > 0
        else 0
    )

    print(f"Current Energy   : {current_energy_kwh:.4f} kWh")
    print(f"Optimized Energy : {optimized_energy_kwh:.4f} kWh")
    print(f"Energy Savings   : {savings_kwh:.4f} kWh")
    print(f"Savings          : {savings_percentage:.2f}%")


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("SmartStreetLightingCompleteAnalytics")
        .getOrCreate()
    )

    print("=" * 60)
    print("SMART STREET LIGHTING - SPARK ANALYTICS")
    print("=" * 60)
    print(f"Input: {HDFS_INPUT}")

    df = load_data(spark)

    record_count = df.count()

    print(f"Records: {record_count:,}")
    print(f"Columns: {len(df.columns)}")

    print("\nSchema:")
    df.printSchema()

    # Complete analytics
    energy_analysis(df)
    motion_power_analysis(df)
    traffic_analysis(df)
    brightness_analysis(df)
    ldr_analysis(df)
    temperature_analysis(df)
    location_energy_analysis(df)
    power_over_time(df)
    fault_detection(df)
    system_health(df)
    energy_optimization(df)

    print("\n" + "=" * 60)
    print("SPARK ANALYTICS COMPLETED")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()