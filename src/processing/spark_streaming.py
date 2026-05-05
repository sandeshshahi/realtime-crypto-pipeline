import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'crypto_trades')

def create_spark_session():
    """
    Initializes a Spark session configured to talk to Kafka.
    """
    # In production, this would connect to a YARN or Kubernetes cluster.
    # Here, 'local[*]' means run locally using all available CPU cores on your Mac.
    return SparkSession.builder \
        .appName("CryptoPulse_RealTime_Aggregator") \
        .master("local[*]") \
        .getOrCreate()

def get_binance_schema():
    """
    Defines the exact structure of the incoming JSON payload.
    Spark requires strict schemas for streaming data.
    """
    return StructType([
        StructField("e", StringType(), True),   # Event type
        StructField("E", LongType(), True),     # Event time (Unix epoch in ms)
        StructField("s", StringType(), True),   # Symbol
        StructField("p", StringType(), True),   # Price (comes as string from Binance)
        StructField("q", StringType(), True),   # Quantity (comes as string)
        StructField("T", LongType(), True)      # Trade time
    ])

def process_stream():
    spark = create_spark_session()
    
    # Spark is incredibly noisy by default. This suppresses INFO logs 
    # so we can actually see our data output.
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session initialized. Connecting to Kafka...")

    # 1. READ FROM KAFKA
    # Kafka sends data as binary key/value pairs. 
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. PARSE THE JSON
    # We cast the binary 'value' column to a string, then parse it using our schema
    schema = get_binance_schema()
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. TYPE CASTING & TIMESTAMP CONVERSION
    # Spark needs actual numbers for math, and a real Timestamp type for windowing
    cleaned_df = parsed_df \
        .withColumn("price", col("p").cast(DoubleType())) \
        .withColumn("quantity", col("q").cast(DoubleType())) \
        .withColumn("trade_timestamp", (col("T") / 1000).cast("timestamp"))

    # 4. WINDOWED AGGREGATION 
    # Group the trades into 1-minute tumbling windows and calculate metrics
    aggregated_df = cleaned_df \
        .withWatermark("trade_timestamp", "1 minute") \
        .groupBy(
            window(col("trade_timestamp"), "1 minute"),
            col("s").alias("symbol")
        ) \
        .agg(
            avg("price").alias("average_price"),
            spark_sum("quantity").alias("total_volume")
        )
    
    # UNPACK THE WINDOW FOR CASSANDRA
    # We must flatten the nested 'window' struct into exact column names that match our Cassandra schema.
    final_df = aggregated_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")

    # 5. OUTPUT TO CONSOLE
    # For now, we just print to the console to verify our logic.
    # todo next we will change this to write to HBase or Cassandra.
    # query = aggregated_df.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    # 5. OUTPUT TO CASSANDRA
    query = final_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/spark_checkpoints_crypto") \
        .start()

    query.awaitTermination()

def write_to_cassandra(batch_df, batch_id):
        # batch_df is a static DataFrame containing just the updated rows for this micro-batch
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "cryptopulse") \
            .option("table", "real_time_aggregates") \
            .mode("append") \
            .save()

if __name__ == "__main__":
    process_stream()