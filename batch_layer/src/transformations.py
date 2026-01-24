from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def get_asset_utilization_scores(df: DataFrame) -> DataFrame:
    """
    TASK 2: Asset Utilization Score
    Calculates Avg Asset Utilization and total Delay Count per Asset_ID.
    """
    return df.groupBy("Asset_ID").agg(
        F.round(F.avg("Asset_Utilization"), 2).alias("avg_utilization_score"),
        F.count(F.when(F.col("Logistics_Delay") == "Yes", 1)).alias("total_delay_count")
    )

def get_top_delay_reasons(df: DataFrame) -> DataFrame:
    """
    TASK 3: Top Delay Reasons
    Identifies the Top 5 most frequent Logistics_Delay_Reason.
    """
    return df.filter(F.col("Logistics_Delay_Reason").isNotNull()) \
             .groupBy("Logistics_Delay_Reason") \
             .count() \
             .orderBy(F.desc("count")) \
             .limit(5)

def get_route_performance_ranking(df: DataFrame) -> DataFrame:
    """
    TASK 4: Route Performance Ranking
    Identifies Top 10 regions with highest average Waiting_Time.
    Uses Spark Window Functions for geographic ranking as mandated.
    """
    # Grouping by Lat/Long to define a unique 'Region'
    region_stats = df.groupBy("Latitude", "Longitude").agg(
        F.avg("Waiting_Time").alias("avg_waiting_time")
    )
    
    # Implementing Window Function for Ranking
    window_spec = Window.orderBy(F.desc("avg_waiting_time"))
    
    return region_stats.withColumn("rank", F.rank().over(window_spec)) \
                       .filter(F.col("rank") <= 10)