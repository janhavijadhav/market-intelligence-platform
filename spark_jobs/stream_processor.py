import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as spark_sum,
    max as spark_max, min as spark_min, count,
    current_timestamp, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType
)
from spark_jobs.snowflake_writer import write_metrics_to_snowflake, write_trades_to_snowflake

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TRADE_SCHEMA = StructType([
    StructField("symbol",      StringType(),  True),
    StructField("price",       DoubleType(),  True),
    StructField("size",        IntegerType(), True),
    StructField("exchange",    StringType(),  True),
    StructField("timestamp",   StringType(),  True),
    StructField("ingested_at", StringType(),  True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("MarketIntelligencePlatform")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_trades(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "market.trades")
        .option("startingOffsets", "latest")
        .load()
    )


def parse_trades(raw_df):
    return (
        raw_df
        .select(
            from_json(
                col("value").cast("string"),
                TRADE_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
        .withColumn("ingested_at", col("ingested_at").cast(TimestampType()))
        .withColumn("trade_value", col("price") * col("size"))
    )


def compute_metrics(parsed_df):
    return (
        parsed_df
        .withWatermark("timestamp", "30 seconds")
        .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"),
            col("symbol")
        )
        .agg(
            spark_round(
                spark_sum("trade_value") / spark_sum("size"), 4
            ).alias("vwap"),
            spark_round(avg("price"), 4).alias("avg_price"),
            spark_round(spark_min("price"), 4).alias("min_price"),
            spark_round(spark_max("price"), 4).alias("max_price"),
            spark_sum("size").alias("total_volume"),
            count("*").alias("trade_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("vwap"),
            col("avg_price"),
            col("min_price"),
            col("max_price"),
            col("total_volume"),
            col("trade_count"),
            current_timestamp().alias("processed_at")
        )
    )


def main():
    logger.info("Starting Spark Streaming job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df    = read_trades(spark)
    parsed_df = parse_trades(raw_df)

    # Stream 1 — write raw trades to Snowflake
    raw_query = (
        parsed_df
        .drop("trade_value")
        .writeStream
        .outputMode("append")
        .foreachBatch(write_trades_to_snowflake)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Stream 2 — write computed metrics to Snowflake
    metrics_df = compute_metrics(parsed_df)
    metrics_query = (
        metrics_df.writeStream
        .outputMode("update")
        .foreachBatch(write_metrics_to_snowflake)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Both streaming queries started. Writing to Snowflake...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
