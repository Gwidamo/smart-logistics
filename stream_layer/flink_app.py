import logging
import json
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor

# --- CONFIGURATION ---
KAFKA_BROKER = "kafka:29092"
INPUT_TOPIC = "logistics_stream"


class StatefulTrafficAlert(KeyedProcessFunction):
    """
    Task 3: Stateful Flink function
    Fires an alert ONLY when an asset enters the 'Heavy' + 'In Transit' state.
    """

    def __init__(self):
        self.critical_state_desc = ValueStateDescriptor("is_critical", Types.BOOLEAN())

    def open(self, runtime_context: RuntimeContext):
        self.is_critical_state = runtime_context.get_state(self.critical_state_desc)

    # REMOVED 'out' argument here to fix the TypeError
    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            asset_id = value.get("Asset_ID")
            traffic = value.get("Traffic_Status")
            shipment = value.get("Shipment_Status")

            # State access
            already_critical = self.is_critical_state.value()
            if already_critical is None:
                already_critical = False

            is_currently_critical = traffic == "Heavy" and shipment == "In Transit"

            # Transition: Normal -> Critical
            if is_currently_critical and not already_critical:
                alert_msg = f"CRITICAL ALERT: Asset {asset_id} is stuck in Heavy Traffic while In Transit!"
                # CHANGED: Use yield instead of out.collect
                yield alert_msg
                self.is_critical_state.update(True)

            # Transition: Critical -> Normal
            elif not is_currently_critical and already_critical:
                # CHANGED: Use yield instead of out.collect
                yield f"INFO: Asset {asset_id} traffic has cleared."
                self.is_critical_state.update(False)

        except Exception as e:
            print(f"Error processing record: {e}")


class WindowAverageFunction(ProcessWindowFunction):
    """
    Task 2: Calculate the 5-minute Tumbling Window Average
    """

    def process(self, key, context, elements):
        count = 0
        total_wait = 0.0
        asset_id = key

        for element in elements:
            try:
                # Ensure we handle potential None types or format errors
                val = element.get("Waiting_Time")
                if val is not None:
                    total_wait += float(val)
                    count += 1
            except ValueError:
                continue

        if count > 0:
            avg_wait = round(total_wait / count, 2)
            window_end = context.window().end
            # OUTPUT TO CONSOLE
            print(
                f"WINDOW REPORT: Asset={asset_id} | AvgWait={avg_wait} | Time={window_end}"
            )
            yield f"Processed {asset_id}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink_logistics_group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    def parse_json(data):
        try:
            return json.loads(data)
        except:
            return None

    # Parse once, reuse stream
    parsed_stream = ds.map(parse_json).filter(lambda x: x is not None)

    # Create the Keyed Stream once
    keyed_stream = parsed_stream.key_by(lambda x: x["Asset_ID"])

    # --- PIPELINE BRANCH 1: ALERTS ---
    # We must capture the output and print it for the Alert to show up
    alert_stream = keyed_stream.process(StatefulTrafficAlert())
    alert_stream.print().name("Alert Sink")

    # --- PIPELINE BRANCH 2: WINDOW ANALYTICS ---
    # Requirement: 5-minute Tumbling Window
    window_stream = keyed_stream.window(
        TumblingProcessingTimeWindows.of(Time.minutes(5))
    ).process(WindowAverageFunction())
    env.execute("Logistics Real-Time Analysis")


if __name__ == "__main__":
    main()
