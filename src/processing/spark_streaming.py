import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, sum as spark_sum
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv


from src.processing.schemas import get_binance_schema
from src.processing.sinks import write_to_cassandra

load_dotenv()
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'crypto_trades')

def create_spark_session():
    builder: SparkSession.Builder = SparkSession.builder # type: ignore
    return (
        builder
        .appName("CryptoPulse_RealTime_Aggregator")
        .master("local[*]")
        .getOrCreate()
    )

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Engine initialized. Reading from Kafka...")

    # READ THE STATIC CSV DATA 
    # We load this into memory once so Spark can quickly look up values
    static_metadata_df = spark.read \
        .option("header", "true") \
        .csv("hdfs://namenode:9000/user/data/crypto_metadata.csv")

    # READ FROM KAFKA
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # PARSE THE JSON (Using our external schema)
    schema = get_binance_schema()
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # TYPE CASTING
    cleaned_df = parsed_df \
        .withColumn("price", col("p").cast(DoubleType())) \
        .withColumn("quantity", col("q").cast(DoubleType())) \
        .withColumn("trade_timestamp", (col("T") / 1000).cast("timestamp"))

    # WINDOWED AGGREGATION
    aggregated_df = cleaned_df \
        .withWatermark("trade_timestamp", "1 minute") \
        .groupBy(window(col("trade_timestamp"), "1 minute"), col("s").alias("symbol")) \
        .agg(
            avg("price").alias("average_price"),
            spark_sum("quantity").alias("total_volume")
        )
    
    final_df = aggregated_df \
        .withColumn("window_start", col("window.start")) \
        .withColumn("window_end", col("window.end")) \
        .drop("window")
    
    # THE STREAM-STATIC JOIN
    # We join our live 1-minute aggregations with the static CSV data based on the "symbol" column
    enriched_df = final_df.join(static_metadata_df, on="symbol", how="left_outer")

    # OUTPUT TO CASSANDRA (Using our external sink)
    query = enriched_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "/tmp/spark_checkpoints_crypto") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    process_stream()