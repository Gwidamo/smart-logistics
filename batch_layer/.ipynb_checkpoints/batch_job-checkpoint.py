from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, row_number
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
        .appName("LogisticsBatchAnalysis") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from local mounted path (mapped to /app/data inside container)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("file:///app/data/raw/smart_logistics_dataset.csv")

    # Task 2: Asset Utilization
    utilization = df.groupBy("Asset_ID").agg(
            avg("Asset_Utilization").alias("Avg_Utilization"),
            count(col("Logistics_Delay")).alias("Total_Delays"))
    
    # Task 3: Top Delay Reasons
    reasons = df.filter(col("Logistics_Delay_Reason").isNotNull()) \
        .groupBy("Logistics_Delay_Reason").count().orderBy(desc("count")).limit(5)

    # Task 4: Route Ranking
    windowSpec = Window.orderBy(desc("Avg_Waiting_Time"))
    routes = df.groupBy("Latitude", "Longitude").agg(avg("Waiting_Time").alias("Avg_Waiting_Time")) \
        .withColumn("Rank", row_number().over(windowSpec)).filter(col("Rank") <= 10)

    # Task 5: Write to HDFS
    utilization.write.mode("overwrite").parquet("hdfs://namenode:9000/output/utilization")
    reasons.write.mode("overwrite").parquet("hdfs://namenode:9000/output/reasons")
    routes.write.mode("overwrite").parquet("hdfs://namenode:9000/output/routes")

    spark.stop()

if __name__ == "__main__":
    main()