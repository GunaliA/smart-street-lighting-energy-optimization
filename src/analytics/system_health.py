from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import count, avg, sum as spark_sum, row_number
HDFS_INPUT = "hdfs://localhost:9000/smart-street-lighting/historical/street_light_data.csv"
SAMPLE_INTERVAL_SECONDS = 1
OUTPUT_PATH = "data/processed/system_health.csv"

def main():
    spark=SparkSession.builder.master("local[*]").appName("SystemHealthSummary").getOrCreate()
    df=spark.read.option("header","true").option("inferSchema","true").csv(HDFS_INPUT)
    energy=df.power_consumption*SAMPLE_INTERVAL_SECONDS/3600.0
    light_status=df.groupBy("light_status").agg(count("*").alias("record_count"),spark_sum(energy).alias("total_energy_kwh"),avg("power_consumption").alias("average_power_w")).orderBy("light_status")
    fault_status=df.groupBy("fault_status").agg(count("*").alias("record_count"),spark_sum(energy).alias("total_energy_kwh"),avg("power_consumption").alias("average_power_w")).orderBy("fault_status")
    latest=df.withColumn("rn",row_number().over(Window.partitionBy("light_id").orderBy(df.timestamp.desc()))).filter("rn=1")
    print("Total Lights:",latest.select("light_id").distinct().count())
    print("Active Lights (latest):",latest.filter("light_status='ON'").select("light_id").count())
    print("Faulty Lights (latest):",latest.filter("fault_status <> 'NORMAL'").select("light_id").count())
    print("Total Energy (kWh):",df.agg((spark_sum(energy)/1000.0).alias("kwh")).first()[0])
    print("Average Power (W):",df.agg(avg("power_consumption")).first()[0])
    light_status.coalesce(1).write.mode("overwrite").option("header","true").csv(OUTPUT_PATH+"/light_status")
    fault_status.coalesce(1).write.mode("overwrite").option("header","true").csv(OUTPUT_PATH+"/fault_status")
    spark.stop()
if __name__ == "__main__": main()
