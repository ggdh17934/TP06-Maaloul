"""Read campus JSON from Kafka and compute event-time counts and attendance."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


CAMPUS_ZONE = "Africa/Algiers"
SCHEMA = StructType([
    StructField("eventId", IntegerType()),
    StructField("studentId", StringType()),
    StructField("eventType", StringType()),
    StructField("department", StringType()),
    StructField("eventTime", StringType()),
])


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="campus-events")
    parser.add_argument("--checkpoint", default="checkpoint/campus-events")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--slide")
    parser.add_argument("--watermark", default="30 seconds")
    parser.add_argument("--trigger", help='Processing-time interval, for example "5 seconds".')
    parser.add_argument("--mode", choices=("counts", "attendance", "event-types"), default="counts")
    parser.add_argument("--available-now", action="store_true", help="Process the current backlog and exit.")
    args = parser.parse_args()
    if args.trigger and args.available_now:
        parser.error("--trigger and --available-now are mutually exclusive")
    return args


def read_events(spark, args):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.broker)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_ts", F.try_to_timestamp("eventTime"))
        .where(
            F.col("event_ts").isNotNull()
            & (F.trim("studentId") != "")
            & (F.trim("department") != "")
            & (F.trim("eventType") != "")
        )
    )


def window_counts(events, args):
    time_window = F.window("event_ts", args.window, args.slide)
    groups = [time_window.alias("window"), "department"]
    if args.mode == "event-types":
        groups.append("eventType")
    else:
        events = events.where(F.col("eventType") == "check-in")
    return events.withWatermark("event_ts", args.watermark).groupBy(*groups).count()


def attendance(checkins):
    # Keep the earliest valid check-in per campus day and student, even when events arrive out of order.
    first = (
        checkins.withColumn("campus_date", F.to_date(F.from_utc_timestamp("event_ts", CAMPUS_ZONE)))
        .groupBy("campus_date", "studentId")
        .agg(F.min(F.struct("event_ts", "eventId", "department", "eventTime")).alias("first"))
        .select("campus_date", "studentId", "first.*")
    )
    local_ts = F.from_utc_timestamp("event_ts", CAMPUS_ZONE)
    cutoff = F.to_timestamp(F.concat_ws(" ", F.col("campus_date"), F.lit("08:15:00")))
    return first.select(
        "campus_date", "studentId", "department", "eventTime",
        F.date_format(local_ts, "HH:mm:ss").alias("campusTime"),
        F.when(local_ts > cutoff, "LATE").otherwise("ON TIME").alias("status"),
    )


def report_progress(progress):
    event_time = progress.get("eventTime", {})
    dropped = sum(state["numRowsDroppedByWatermark"] for state in progress["stateOperators"])
    print(
        f'Progress batch={progress["batchId"]} utc={progress["timestamp"]} '
        f'input={progress["numInputRows"]} processed/s={progress["processedRowsPerSecond"]:.2f} '
        f'maxEventTime={event_time.get("max", "-")} '
        f'watermark={event_time.get("watermark", "-")} dropped={dropped}',
        flush=True,
    )


def main():
    args = options()
    spark = (
        SparkSession.builder.appName(f"CampusRealTimeAnalytics-{args.mode}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    events = read_events(spark, args)
    if args.mode == "attendance":
        result = attendance(events.where(F.col("eventType") == "check-in"))
    else:
        result = window_counts(events, args)
    checkpoint = Path(args.checkpoint).resolve().as_uri()
    writer = (
        result.writeStream.outputMode("update").format("console")
        .option("truncate", "false").option("numRows", "30")
        .option("checkpointLocation", checkpoint)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    elif args.trigger:
        writer = writer.trigger(processingTime=args.trigger)
    query = writer.start()
    print(f"Ready Spark {spark.version} | {args.mode} | {args.topic} | {args.checkpoint}", flush=True)
    seen = set()
    try:
        while True:
            finished = query.awaitTermination(1)
            for progress in query.recentProgress:
                key = (progress["batchId"], progress["timestamp"])
                if key not in seen:
                    report_progress(progress)
            seen = {(p["batchId"], p["timestamp"]) for p in query.recentProgress}
            if finished:
                break
    except KeyboardInterrupt:
        pass
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
