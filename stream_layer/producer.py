import time
import json
import csv
import logging
import os
from datetime import datetime  # <--- NEW IMPORT
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LogisticsProducer")

# CONFIGURATION
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = "logistics_stream"
DATA_FILE = "/app/data/smart_logistics_dataset.csv"

# --- CONFIG: DATE FORMAT ---
# Adjust this to match the format in your CSV file
# Common formats: "%Y-%m-%d %H:%M:%S" or "%Y-%m-%dT%H:%M:%S"
CSV_TIMESTAMP_COLUMN = "Timestamp"
CSV_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def reset_topic():
    """
    Deletes the existing topic and recreates it to ensure a clean slate.
    """
    logger.info(f"--- RESETTING TOPIC: {TOPIC_NAME} ---")

    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

        # 1. Delete if exists
        existing_topics = admin_client.list_topics()
        if TOPIC_NAME in existing_topics:
            logger.info(f"Deleting existing topic '{TOPIC_NAME}'...")
            admin_client.delete_topics([TOPIC_NAME])
            time.sleep(3)

        # 2. Create fresh topic
        logger.info(f"Creating fresh topic '{TOPIC_NAME}'...")
        new_topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logger.info("Topic reset successful.")

    except Exception as e:
        logger.warning(f"Topic reset warning (non-fatal): {e}")
    finally:
        if admin_client:
            admin_client.close()


def main():
    logger.info(f"--- STARTING ENDLESS STREAM PRODUCER ---")
    logger.info(f"Target Broker: {KAFKA_BROKER}")

    # --- STEP 1: Wait for Broker to be ready ---
    connected = False
    for i in range(15):
        try:
            temp_producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
            temp_producer.close()
            connected = True
            break
        except Exception:
            logger.warning(f"Waiting for Kafka... Attempt {i + 1}/15")
            time.sleep(5)

    if not connected:
        logger.error("Could not connect to Kafka. Exiting.")
        return

    # --- STEP 2: Clear/Reset the Topic ---
    reset_topic()

    # --- STEP 3: Initialize Producer ---
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER], value_serializer=json_serializer
    )
    logger.info("Producer initialized.")

    try:
        logger.info("Starting new pass through the dataset...")

        with open(DATA_FILE, "r") as f:
            reader = csv.DictReader(f)

            for row in reader:
                # --- NEW LOGIC START ---
                ts_ms = None
                try:
                    # 1. Parse string to datetime
                    date_str = row[CSV_TIMESTAMP_COLUMN]
                    dt_obj = datetime.strptime(date_str, CSV_DATE_FORMAT)

                    # 2. Convert to Unix milliseconds (int)
                    ts_ms = int(dt_obj.timestamp() * 1000)
                except KeyError:
                    logger.warning(f"Column '{CSV_TIMESTAMP_COLUMN}' not found in row.")
                except ValueError as ve:
                    logger.warning(f"Date format mismatch: {ve}")

                # 3. Send with explicit timestamp
                producer.send(TOPIC_NAME, row, timestamp_ms=ts_ms)
                # --- NEW LOGIC END ---

                time.sleep(0.5)

        producer.flush()

    except KeyboardInterrupt:
        logger.info("Stream stopped by user.")
    except Exception as e:
        logger.error(f"Stream Error: {e}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
