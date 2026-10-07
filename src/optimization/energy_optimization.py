from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, when, sum as spark_sum
HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/historical/street_light_data.csv"
SAMPLE_INTERVAL_SECONDS = 1

def optimized_brightness_expr(df):
    return when(col("fault_status")=="LIGHT_FAILURE",0).when(col("ldr_value")>=700,30).when((col("traffic_level")=="LOW")&(col("motion_detected")==0),40).when((col("traffic_level")=="LOW")&(col("motion_detected")==1),50).when((col("traffic_level")=="MEDIUM")&(col("motion_detected")==0),60).when((col("traffic_level")=="MEDIUM")&(col("motion_detected")==1),75).when((col("traffic_level")=="HIGH")&(col("motion_detected")==0),85).otherwise(100)

def main():
    spark=SparkSession.builder.master("local[*]").appName("StreetLightingEnergyOptimization").getOrCreate()
    df=spark.read.option("header","true").option("inferSchema","true").csv(HDFS_INPUT)
    opt=df.withColumn("optimized_brightness",optimized_brightness_expr(df)).withColumn("optimized_power",when(col("brightness_level")==0,0.0).otherwise(col("power_consumption")*col("optimized_brightness")/col("brightness_level"))).withColumn("current_energy_kwh",col("power_consumption")*SAMPLE_INTERVAL_SECONDS/3_600_000.0).withColumn("optimized_energy_kwh",col("optimized_power")*SAMPLE_INTERVAL_SECONDS/3_600_000.0)
    totals=opt.agg(spark_sum("current_energy_kwh").alias("current_kwh"),spark_sum("optimized_energy_kwh").alias("optimized_kwh")).first()
    current=float(totals.current_kwh or 0); optimized=float(totals.optimized_kwh or 0); savings=current-optimized; pct=savings/current*100 if current else 0
    print(f"Current energy: {current:,.4f} kWh"); print(f"Optimized energy: {optimized:,.4f} kWh"); print(f"Estimated savings: {savings:,.4f} kWh ({pct:.2f}%)")
    opt.groupBy("traffic_level").agg(avg("power_consumption").alias("current_average_power_w"),avg("optimized_power").alias("optimized_average_power_w")).orderBy("traffic_level").show()
    spark.stop()
if __name__ == "__main__": main()
