import json
import sys
import os
from pyspark.sql import SparkSession

# Add src to path to ensure modules are discoverable
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from logger_config import setup_logging
from transformations import (
    get_asset_utilization_scores, 
    get_top_delay_reasons, 
    get_route_performance_ranking
)

# 1. Initialize Professional Logger
logger = setup_logging("LogisticsBatchLayer")

def load_config(config_path='/app/batch_layer/config.json'):
    """Loads configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        sys.exit(1)

def main():
    # 2. Load Environment Configurations
    config = load_config()
    
    # TASK 1: Data Setup & Ingestions
    # Using config values for portability
    spark = SparkSession.builder \
        .appName(config["app_name"]) \
        .master(config["spark_master"]) \
        .config("spark.hadoop.fs.defaultFS", config["hdfs_base"]) \
        .getOrCreate()

    try:
        logger.info("--- BATCH PIPELINE INITIALIZED ---")
        
        # Define HDFS Paths from Config
        raw_path = config["raw_data_path"]
        output_base = config["output_base_path"]

        # Ingestion
        logger.info(f"Ingesting raw data from: {config['hdfs_base']}{raw_path}")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
        
        record_count = df.count()
        logger.info(f"Data ingestion complete. Total records: {record_count}")

        # --- Transformation Layer ---
        
        # TASK 2: Asset Utilization Score
        logger.info("Calculating Task 2: Asset Utilization and Historical Delays...")
        scores_df = get_asset_utilization_scores(df)

        # TASK 3: Top Delay Reasons
        logger.info("Calculating Task 3: Identifying Top 5 Delay Factors...")
        reasons_df = get_top_delay_reasons(df)

        # TASK 4: Route Performance Ranking (Window Functions)
        logger.info("Calculating Task 4: Regional Performance Ranking (Geo-Grouping)...")
        rankings_df = get_route_performance_ranking(df)

        # --- Storage Layer ---
        
        # TASK 5: Batch Result Storage
        # Storing in professional Parquet format
        logger.info(f"Persisting results to HDFS output directory: {output_base}")
        
        scores_df.write.mode("overwrite").parquet(f"{output_base}asset_scores")
        reasons_df.write.mode("overwrite").parquet(f"{output_base}top_delays")
        rankings_df.write.mode("overwrite").parquet(f"{output_base}route_rankings")

        logger.info("--- BATCH PIPELINE SUCCESSFUL ---")

    except Exception as e:
        logger.error(f"CRITICAL ERROR during pipeline execution: {str(e)}", exc_info=True)
    finally:
        logger.info("Terminating SparkSession and releasing cluster resources.")
        spark.stop()

if __name__ == "__main__":
    main()