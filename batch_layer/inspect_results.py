from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ResultInspector") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

def show_report(title, path):
    print(f"\n{'='*60}")
    print(f" REPORT: {title}")
    print(f"{'='*60}")
    try:
        spark.read.parquet(path).show(10, truncate=False)
    except Exception:
        print(f"Error: Data at {path} not found.")

show_report("ASSET UTILIZATION & DELAY COUNTS", "/user/refined/asset_scores")
show_report("TOP 5 DELAY REASONS", "/user/refined/top_delays")
show_report("TOP 10 REGIONAL WAITING TIMES (GEOGRAPHIC RANKING)", "/user/refined/route_rankings")

spark.stop()