#!/bin/bash

echo "Starting HDFS Bootstrap..."

# Loop until the NameNode RPC port (8020) is active
while ! nc -z localhost 8020; do   
  echo "Waiting for NameNode RPC (8020) to be available..."
  sleep 5
done

echo "NameNode is up! Checking for Safe Mode..."

# Wait until NameNode leaves Safe Mode
hdfs dfsadmin -safemode wait

echo "Creating directory structure..."
hdfs dfs -mkdir -p /user/raw/logistics

if [ -f "/app/data/smart_logistics_dataset.csv" ]; then
    echo "Uploading smart_logistics_dataset.csv to HDFS..."
    hdfs dfs -put -f /app/data/smart_logistics_dataset.csv /user/raw/logistics/
    echo "Bootstrap Complete. Data is in the Lake."
else
    echo "ERROR: Local file /app/data/smart_logistics_dataset.csv not found!"
fi