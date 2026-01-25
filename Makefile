# Makefile for Smart Logistics Big Data Project

# ==============================================================================
# HELP
# ==============================================================================
.PHONY: help
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  up                      - Start the infrastructure"
	@echo "  down                    - Stop the infrastructure"
	@echo "  check-hdfs              - Verify NameNode is out of Safe Mode"
	@echo "  setup-hdfs              - Create HDFS directory structure"
	@echo "  upload-data             - Upload the local CSV to HDFS"
	@echo "  verify-upload           - Verify the file is in HDFS"
	@echo "  submit-batch            - Submit the batch job to Spark"
	@echo "  list-refined            - List the refined analytics tables"
	@echo "  batch-report            - Run inspector and generate batch report"
	@echo "  "
	@echo "  start-producer          - Start the Kafka producer for streaming"
	@echo "  submit-stream           - Submit the Flink streaming job"
	@echo "  stream-logs             - Follow the logs of the Flink Task Manager"
	@echo "  stream-report           - Generate stream analytics report"
	@echo "  "
	@echo "  full-batch-pipeline     - Run the full batch pipeline (setup, upload, submit, report)"
	@echo "  full-stream-pipeline    - Run the full stream pipeline (start producer, submit, report)"


# ==============================================================================
# INFRASTRUCTURE
# ==============================================================================
.PHONY: up
up:
	docker compose up -d

.PHONY: down
down:
	docker compose down
# ==============================================================================
# BATCH LAYER
# ==============================================================================
.PHONY: check-hdfs
check-hdfs:
	docker exec -it namenode hdfs dfsadmin -report

.PHONY: setup-hdfs
setup-hdfs:
	docker exec -it namenode hdfs dfs -mkdir -p /user/raw/logistics

.PHONY: upload-data
upload-data:
	docker exec -it namenode hdfs dfs -put -f /app/data/smart_logistics_dataset.csv /user/raw/logistics/

.PHONY: verify-upload
verify-upload:
	docker exec -it namenode hdfs dfs -ls /user/raw/logistics/

.PHONY: submit-batch
submit-batch:
	docker exec -u 0 -it spark-worker pip3 install geopy
	docker exec -u 0 -it spark-master pip3 install geopy
	docker exec -it spark-master /spark/bin/spark-submit \
   --master spark://spark-master:7077 \
   --py-files /app/batch_layer/src/transformations.py \
   /app/batch_layer/batch_job.py
.PHONY: list-refined
list-refined:
	docker exec -it namenode hdfs dfs -ls -R /user/refined/

.PHONY: batch-report
batch-report:
	docker exec -e PYTHONIOENCODING=utf-8 -it spark-master /spark/bin/spark-submit /app/batch_layer/inspect_results.py > output/batch_analysis_report.txt

.PHONY: full-batch-pipeline
full-batch-pipeline: setup-hdfs upload-data submit-batch batch-report


# ==============================================================================
# STREAM LAYER
# ==============================================================================
.PHONY: start-producer
start-producer:
	docker compose start kafka-producer

.PHONY: submit-stream
submit-stream:
	docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /app/stream_layer/flink_app.py

.PHONY: stream-logs
stream-logs:
	docker logs -f flink-taskmanager

.PHONY: stream-report
stream-report:
	docker logs flink-taskmanager > output/stream_analytics_report.txt

.PHONY: full-stream-pipeline
full-stream-pipeline: start-producer submit-stream
