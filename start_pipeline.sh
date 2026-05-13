#!/usr/bin/env bash


echo "=== Setting up Java 17 ==="

OS="$(uname -s)"
JAVA17_FOUND=false

# ─── macOS ───────────────────────────────────────────────
if [ "$OS" = "Darwin" ]; then
  # Apple Silicon Homebrew
  if [ -d "/opt/homebrew/opt/openjdk@17" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    JAVA17_FOUND=true
  # Intel Mac Homebrew
  elif [ -d "/usr/local/opt/openjdk@17" ]; then
    export JAVA_HOME="/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    JAVA17_FOUND=true
  # Fallback: use macOS java_home utility
  elif /usr/libexec/java_home -v 17 &>/dev/null; then
    export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
    JAVA17_FOUND=true
  fi

# ─── Linux ───────────────────────────────────────────────
elif [ "$OS" = "Linux" ]; then
  # Homebrew on Linux (typically installed at /home/linuxbrew)
  if [ -d "/home/linuxbrew/.linuxbrew/opt/openjdk@17" ]; then
    export JAVA_HOME="/home/linuxbrew/.linuxbrew/opt/openjdk@17"
    JAVA17_FOUND=true
  # Standard Linux package manager paths
  elif [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    JAVA17_FOUND=true
  elif [ -d "/usr/lib/jvm/java-17-openjdk" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk"
    JAVA17_FOUND=true
  # Wildcard fallback for any java-17 install
  else
    JVM_PATH=$(find /usr/lib/jvm -maxdepth 1 -name "java-17*" -type d 2>/dev/null | head -n 1)
    if [ -n "$JVM_PATH" ]; then
      export JAVA_HOME="$JVM_PATH"
      JAVA17_FOUND=true
    fi
  fi
fi

# ─── Final Check ─────────────────────────────────────────
if [ "$JAVA17_FOUND" = true ] && [ -f "$JAVA_HOME/bin/java" ]; then
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "✅ JAVA_HOME set to: $JAVA_HOME"
  java -version
else
  echo "❌ Java 17 is not found on this system."
  echo ""
  echo "Please install it:"
  if [ "$OS" = "Darwin" ]; then
    echo "  brew install openjdk@17"
  elif [ "$OS" = "Linux" ]; then
    echo "  Homebrew : brew install openjdk@17"
    echo "  Ubuntu   : sudo apt install openjdk-17-jdk"
    echo "  Fedora   : sudo dnf install java-17-openjdk"
  fi
  exit 1
fi


echo "Setting Python Path to root directory..."
export PYTHONPATH=$(pwd)

echo "Activating Virtual Environment..."
source venv/bin/activate

export SPARK_HOME="$(python3 -c 'import pyspark; import os; print(os.path.dirname(pyspark.__file__))')"
export PATH="$SPARK_HOME/bin:$PATH"


echo "Initializing Kafka Topic..."
docker exec -it kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic crypto_trades \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Initializing Cassandra Schema..."
docker exec -i cassandra cqlsh < src/cassandra/schema.cql

echo "Uploading metadata CSV to HDFS..."
docker cp data/crypto_metadata.csv namenode:/tmp/crypto_metadata.csv
docker exec namenode hdfs dfs -mkdir -p /user/data
docker exec namenode hdfs dfs -put -f /tmp/crypto_metadata.csv /user/data/crypto_metadata.csv

echo "Resolving HDFS namenode IP for host-side Spark..."
export HDFS_NAMENODE="localhost:9000"

echo "Waiting for Schema Registry to be ready..."
until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
  echo "  Schema Registry not ready yet, retrying in 3s..."
  sleep 3
done
echo "✅ Schema Registry is ready."

echo "Clearing stale Spark checkpoints..."
rm -rf /tmp/spark_checkpoints_crypto

echo "Starting Binance Producer..."
python -m src.ingestion.binance_producer &
PRODUCER_PID=$!

echo "Waiting for Avro schema to be registered in Schema Registry..."
until curl -s http://localhost:8081/subjects/crypto_trades-value/versions/latest > /dev/null 2>&1; do
  echo "  Schema not registered yet (waiting for first message), retrying in 3s..."
  sleep 3
done
echo "✅ Avro schema registered. Starting Spark..."

echo "Starting Spark Streaming Engine..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 \
  --conf spark.cassandra.connection.host=127.0.0.1 \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  src/processing/spark_streaming.py &
SPARK_PID=$!

echo "Pipeline is running in the background!"
echo "Press [Ctrl+C] to safely shut down both processes."

trap "echo '🛑 Shutting down pipeline...'; kill -9 $PRODUCER_PID $SPARK_PID 2>/dev/null; exit 0" INT
wait