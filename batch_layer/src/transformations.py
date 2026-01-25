from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from geopy.geocoders import Nominatim
import time


def get_asset_utilization_scores(df: DataFrame) -> DataFrame:
    """
    TASK 2: Asset Utilization Score
    Calculates Avg Asset Utilization and total Delay Count per Asset_ID.
    """
    return (
        df.groupBy("Asset_ID")
        .agg(
            F.round(F.avg("Asset_Utilization"), 2).alias("avg_utilization_score"),
            F.count(F.when(F.col("Logistics_Delay") == 1, 1)).alias(
                "total_delay_count"
            ),
        )
        .sort("Asset_ID")
    )


def get_top_delay_reasons(df: DataFrame) -> DataFrame:
    """
    TASK 3: Top Delay Reasons
    Identifies the Top 5 most frequent Logistics_Delay_Reason.
    """
    return (
        df.filter(F.col("Logistics_Delay_Reason").isNotNull())
        .filter(F.col("Logistics_Delay") == 1)
        .filter(F.col("Logistics_Delay_Reason") != "None")
        .groupBy("Logistics_Delay_Reason")
        .count()
        .orderBy(F.desc("count"))
        .limit(5)
    )


def get_city_online(lat, lon):
    """
    Performs reverse geocoding to get City from coordinates using Nominatim.
    Includes a safety delay to prevent API banning.
    FORCE-FIX: Returns 'Unknown Region' if the result is not in English/ASCII.
    """
    try:
        # 1. Initialize API
        geolocator = Nominatim(user_agent="logistics_student_project_v1")

        # 2. SAFETY DELAY
        time.sleep(1.1)

        # 3. Lookup
        coords = f"{lat}, {lon}"
        # Request English, but OSM might ignore this if no English tag exists
        location = geolocator.reverse(language="en", query=coords, exactly_one=True)

        if location:
            address = location.raw.get("address", {})
            # Check multiple fields
            city = (
                address.get("city")
                or address.get("town")
                or address.get("village")
                or address.get("county")
            )

            if city:
                # --- SANITIZATION CHECK ---
                # This checks if all characters are standard English (ASCII).
                # If it contains Cyrillic/Chinese/Arabic, this returns False.
                if all(ord(c) < 128 for c in city):
                    return city
                else:
                    return f"Region ({lat:.1f}, {lon:.1f})"  # Fallback to coordinates if name is unreadable

            return "Unknown Location"
        else:
            return "Location not found"

    except Exception:
        return "Lookup Error"


# --- 2. Register as Spark UDF ---
city_lookup_udf = F.udf(get_city_online, StringType())


# --- 3. The Main Transformation Function ---
def get_route_performance_ranking(df: DataFrame) -> DataFrame:
    """
    TASK 4: Route Performance Ranking (Enriched with City Names)
    """

    # 1. Enrich DataFrame with City Name
    df_enriched = df.withColumn(
        "City", city_lookup_udf(F.col("Latitude"), F.col("Longitude"))
    )

    # 2. Group by the new 'City' name
    city_stats = df_enriched.groupBy("City").agg(
        F.avg("Waiting_Time").alias("avg_waiting_time")
    )

    # 3. Rank the cities
    window_spec = Window.orderBy(F.desc("avg_waiting_time"))

    return city_stats.withColumn("rank", F.rank().over(window_spec)).filter(
        F.col("rank") <= 10
    )
