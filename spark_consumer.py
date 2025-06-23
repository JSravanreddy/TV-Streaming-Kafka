from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TV Event Metrics to Prometheus") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read Kafka Stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tv-events") \
    .load()

# Parse Kafka Data
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

schema = StructType() \
    .add("event", StringType()) \
    .add("region", StringType()) \
    .add("channel_id", StringType()) \
    .add("errorCode", StringType()) \
    .add("watchDurationSec", IntegerType()) \
    .add("timestamp", TimestampType())

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Prometheus setup
registry = CollectorRegistry()
live_events_gauge = Gauge('live_events_total', 'Total live events received', registry=registry)
top_regions_gauge = Gauge('top_regions', 'Top regions by event count', ['region'], registry=registry)
top_channels_gauge = Gauge('top_channels', 'Top TV channels', ['channel'], registry=registry)
error_events_gauge = Gauge('error_event_count', 'Number of error events', registry=registry)
avg_watch_duration_gauge = Gauge('avg_watch_duration', 'Average watch duration', registry=registry)

# Metric Processor
def process_metrics(df, epoch_id):
    df.persist()

    # 🔁 Total Live Events
    total_events = df.count()
    live_events_gauge.set(total_events)

    # 🌍 Top Regions
    region_counts = df.groupBy("region").count().orderBy("count", ascending=False).limit(3).collect()
    for row in region_counts:
        top_regions_gauge.labels(region=row["region"]).set(row["count"])

    # 📺 Top Channels
    top_channels = df.filter(col("channel_id").isNotNull()) \
        .groupBy("channel_id").count().orderBy("count", ascending=False).limit(3).collect()
    for row in top_channels:
        top_channels_gauge.labels(channel=row["channel_id"]).set(row["count"])

    # ⚠️ Error Event Count
    error_count = df.filter(col("event") == "error_event").count()
    error_events_gauge.set(error_count)

    # ⏱️ Average Watch Duration
    avg_duration = df.select(avg("watchDurationSec")).first()[0]
    if avg_duration:
        avg_watch_duration_gauge.set(avg_duration)

    # ✅ Push to Pushgateway
    push_to_gateway('localhost:9091', job='tv_event_metrics', registry=registry)

    print(f"\n✅ Metrics pushed at {time.strftime('%X')}")
    df.unpersist()

# Start the Stream
query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_metrics) \
    .start()

query.awaitTermination()
