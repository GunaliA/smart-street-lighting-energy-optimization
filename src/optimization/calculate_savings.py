from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum
HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/historical/street_light_data.csv"
SAMPLE_INTERVAL_SECONDS = 1

spark=SparkSession.builder.master("local[*]").appName("EnergySavings").getOrCreate()
df=spark.read.option("header","true").option("inferSchema","true").csv(HDFS_INPUT)
opt=df.withColumn("optimized_brightness",when(col("fault_status")=="LIGHT_FAILURE",0).when(col("ldr_value")>=700,30).when((col("traffic_level")=="LOW")&(col("motion_detected")==0),40).when((col("traffic_level")=="LOW")&(col("motion_detected")==1),50).when((col("traffic_level")=="MEDIUM")&(col("motion_detected")==0),60).when((col("traffic_level")=="MEDIUM")&(col("motion_detected")==1),75).when((col("traffic_level")=="HIGH")&(col("motion_detected")==0),85).otherwise(100)).withColumn("optimized_power",when(col("brightness_level")==0,0.0).otherwise(col("power_consumption")*col("optimized_brightness")/col("brightness_level")))
row=opt.select((spark_sum("power_consumption")*SAMPLE_INTERVAL_SECONDS/3_600_000.0).alias("current_kwh"),(spark_sum("optimized_power")*SAMPLE_INTERVAL_SECONDS/3_600_000.0).alias("optimized_kwh")).first()
current=float(row.current_kwh or 0); optimized=float(row.optimized_kwh or 0); savings=current-optimized; percentage=savings/current*100 if current else 0
print(f"Current energy: {current:,.4f} kWh"); print(f"Optimized energy: {optimized:,.4f} kWh"); print(f"Estimated savings: {savings:,.4f} kWh"); print(f"Estimated reduction: {percentage:.2f}%")
spark.stop()
