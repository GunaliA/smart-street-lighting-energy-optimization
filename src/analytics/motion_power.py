from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as spark_sum, count, when
HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/historical/street_light_data.csv"
SAMPLE_INTERVAL_SECONDS = 1
OUTPUT_PATH = "data/processed/motion_power.csv"

def main():
    spark=SparkSession.builder.master("local[*]").appName("MotionVsPowerAnalysis").getOrCreate()
    df=spark.read.option("header","true").option("inferSchema","true").csv(HDFS_INPUT)
    result=(df.withColumn("motion_status",when(df.motion_detected==1,"Motion Detected").otherwise("No Motion")).groupBy("motion_status").agg(count("*").alias("record_count"),spark_sum(df.power_consumption*SAMPLE_INTERVAL_SECONDS/3600.0).alias("total_energy_kwh"),avg("power_consumption").alias("average_power_w")).orderBy("motion_status"))
    result.show(truncate=False); result.coalesce(1).write.mode("overwrite").option("header","true").csv(OUTPUT_PATH); spark.stop()
if __name__ == "__main__": main()
