import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    window, count, min as fmin, max as fmax, avg, stddev
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--inputTopic", default="input-topic")
    p.add_argument("--outputTopic", default="output-topic")
    p.add_argument("--windowDuration", default="5 minutes")
    p.add_argument("--slideDuration", default=None)  # if set => sliding, else tumbling
    p.add_argument("--checkpoint", default="/tmp/spark-checkpoints/main-app")
    return p.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("bd-stream-main")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Adjust schema to your dataset.
    # REQUIRED: event_time (timestamp), group_key (string), value (double)
    schema = StructType([
        StructField("event_time", TimestampType(), True),
        StructField("group_key", StringType(), True),
        StructField("value", DoubleType(), True),
    ])

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.inputTopic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("r"))
        .select("r.*")
        .where(col("event_time").isNotNull())
        .where(col("group_key").isNotNull())
    )

    # Watermark helps state cleanup for windows
    base = parsed.withWatermark("event_time", "10 minutes")

    if args.slideDuration:
        grouped = base.groupBy(
            window(col("event_time"), args.windowDuration, args.slideDuration),
            col("group_key")
        )
    else:
        grouped = base.groupBy(
            window(col("event_time"), args.windowDuration),
            col("group_key")
        )

    agg = grouped.agg(
        count("*").alias("count"),
        fmin("value").alias("min_val"),
        fmax("value").alias("max_val"),
        avg("value").alias("avg_val"),
        stddev("value").alias("stddev_val"),
    )

    out = agg.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("group_key"),
        col("count"),
        col("min_val"),
        col("max_val"),
        col("avg_val"),
        col("stddev_val"),
    )

    kafka_out = out.selectExpr(
        "CAST(group_key AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    query = (
        kafka_out.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", args.outputTopic)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
