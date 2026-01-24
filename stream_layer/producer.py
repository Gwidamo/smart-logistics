import time
import json
import csv
import logging
import os
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LogisticsProducer")

# CONFIGURATION
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = "logistics_stream"
DATA_FILE = "/app/data/smart_logistics_dataset.csv"

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def main():
    logger.info(f"--- STARTING ENDLESS STREAM PRODUCER ---")
    logger.info(f"Target Broker: {KAFKA_BROKER}")

    # Retry Loop for Connection
    producer = None
    for i in range(15):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer
            )
            logger.info("Successfully connected to Kafka!")
            break
        except Exception as e:
            logger.warning(f"Waiting for Kafka... Attempt {i+1}/15")
            time.sleep(5)
    
    if not producer:
        return

    try:
        # INFINITE LOOP: Re-open the file every time we finish it
        while True:
            logger.info("Starting new pass through the dataset...")
            
            with open(DATA_FILE, "r") as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Update Timestamp to NOW
                    row['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    producer.send(TOPIC_NAME, row)
                    
                    # Sleep to control speed (0.5s = 2 messages per second)
                    time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Stream stopped by user.")
    except Exception as e:
        logger.error(f"Stream Error: {e}")
    finally:
        if producer: producer.close()

if __name__ == "__main__":
    main()