import logging
import json
import psycopg2 # <--- Library to talk to Postgres
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction, ProcessFunction

# --- CONFIGURATION ---
KAFKA_BROKER = "kafka:29092"
INPUT_TOPIC = "logistics_stream"
# Database Connection Config
DB_HOST = "postgres"
DB_NAME = "logistics_db"
DB_USER = "myuser"
DB_PASS = "mypassword"

class AlertFunction(ProcessFunction):
    """ TASK B.3: High-Traffic Alerting """
    def process_element(self, value, ctx):
        traffic = value.get("Traffic_Status")
        status = value.get("Shipment_Status")
        asset_id = value.get("Asset_ID")
        
        if traffic == "Heavy" and status == "In Transit":
            print(f"ALERT: High Traffic for {asset_id} at {value.get('Timestamp')}")

class WindowAverageFunction(ProcessWindowFunction):
    """ TASK B.2: Real-Time Waiting Time -> WRITE TO POSTGRES """
    def process(self, key, context, elements):
        count = 0
        total_wait = 0.0
        asset_id = key 
        
        for element in elements:
            try:
                total_wait += float(element.get("Waiting_Time", 0))
                count += 1
            except ValueError:
                continue

        if count > 0:
            avg_wait = round(total_wait / count, 2)
            
            # --- WRITE TO DATABASE ---
            try:
                conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
                cur = conn.cursor()
                query = "INSERT INTO window_analytics (asset_id, avg_wait_time) VALUES (%s, %s)"
                cur.execute(query, (asset_id, avg_wait))
                conn.commit()
                cur.close()
                conn.close()
                print(f"DB SAVED: Asset={asset_id} | AvgWait={avg_wait}")
            except Exception as e:
                print(f"DB ERROR: {e}")
            
            yield f"Saved {asset_id} to DB"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) 

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKER) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink_logistics_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    def parse_json(data):
        try: return json.loads(data)
        except: return None

    parsed_stream = ds.map(parse_json).filter(lambda x: x is not None)

    # Alerts (Still print to console)
    parsed_stream.process(AlertFunction())

    # Window -> Postgres
    parsed_stream \
        .key_by(lambda x: x["Asset_ID"]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(30))) \
        .process(WindowAverageFunction())

    env.execute("Logistics DB Analysis")

if __name__ == "__main__":
    main()