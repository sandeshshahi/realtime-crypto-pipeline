#!/bin/bash

echo "Enforcing Homebrew Java 17 for Spark compatibility..."
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

echo "Setting Python Path to root directory..."
export PYTHONPATH=$(pwd)

echo "Activating Virtual Environment..."
source venv/bin/activate

echo "Initializing Kafka Topic..."
docker exec -it kafka kafka-topics \
  --create \
  --topic crypto_trades \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Starting Binance Producer..."
python -m src.ingestion.binance_producer &
PRODUCER_PID=$!

echo "Starting Spark Streaming Engine..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.cassandra.connection.host=127.0.0.1 \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  src/processing/spark_streaming.py &
SPARK_PID=$!

echo "Pipeline is running in the background!"
echo "Press [Ctrl+C] to safely shut down both processes."

trap "echo '🛑 Shutting down pipeline...'; kill $PRODUCER_PID $SPARK_PID; exit" INT
wait