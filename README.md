# Smart Logistics Big Data Project

This project demonstrates a big data processing pipeline for smart logistics, using a combination of batch and stream processing. The infrastructure is managed with Docker Compose, and the processing is done with Apache Spark and Apache Flink.

## Project Structure

- `docker-compose.yml`: Defines the services for the big data infrastructure, including Hadoop HDFS, Spark, Flink, and Kafka.
- `batch_layer/`: Contains the code for the batch processing layer, which uses Spark to process the historical logistics data.
- `stream_layer/`: Contains the code for the stream processing layer, which uses Flink to process real-time logistics data from Kafka.
- `data/`: Contains the raw data used in the project.
- `Makefile`: Provides a set of commands to automate the project's workflow.

## Getting Started

### Prerequisites

- Docker and Docker Compose must be installed on your system.

### 1. Start the Infrastructure

To start all the necessary services, run the following command:

```bash
make up
```

This will start the Hadoop HDFS, Spark, Flink, and Kafka services in the background.

### 2. Verify HDFS

Before proceeding, ensure that the HDFS NameNode is out of safe mode and the RPC service is active:

```bash
make check-hdfs
```

## Batch Processing

The batch layer processes the `smart_logistics_dataset.csv` file.

### 1. Setup HDFS Directory

Create the necessary directory in HDFS for the raw data:

```bash
make setup-hdfs
```

### 2. Upload Data to HDFS

Upload the local CSV file to the HDFS directory:

```bash
make upload-data
```

You can verify the file has been uploaded successfully with:

```bash
make verify-upload
```

### 3. Submit the Batch Job

Submit the Spark batch job to process the data:

```bash
make submit-batch
```

### 4. Inspect the Results

After the job completes, you can list the refined analytics tables in HDFS:

```bash
make list-refined
```

To generate a detailed analysis report, run the inspector script:

```bash
make batch-report
```

This will create a `ouptut/batch_analysis_report.txt` file in the project's root directory.

### Run the Full Batch Pipeline

You can run all the batch processing steps sequentially with a single command:

```bash
make full-batch-pipeline
```

## Stream Processing

The stream layer processes real-time data using Flink and Kafka.

### 1. Start the Kafka Producer

To begin the stream of data, start the Kafka producer:

```bash
make start-producer
```

### 2. Submit the Flink Job

Submit the Flink streaming job to process the data from Kafka:

```bash
make submit-stream
```

### 3. Monitor the Stream

You can monitor the logs of the Flink Task Manager to see the real-time processing:

```bash
make stream-logs
```

### 4. Generate a Stream Report

To capture the output of the streaming analytics, run:

```bash
make stream-report
```

This will create a `output/stream_analytics_report.txt` file.

### Run the Full Stream Pipeline

You can run all the stream processing steps with:

```bash
make full-stream-pipeline
```

## Stopping the Infrastructure

To stop all the running services, use:

```bash
make down
```

## 👥 Team Members
Mohamed Habiba
Ahmed Tarek Aboughanima
Mohamed Atef Omara
Mohamed Abdelsalam Farag Gwida

