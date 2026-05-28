from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

# ── THRESHOLDS — must match traffic_producer.py ───────────────────────────────
RATIO_HIGH   = 0.60
RATIO_MEDIUM = 0.80
ABS_HIGH     = 12.0   # km/h
ABS_MEDIUM   = 20.0   # km/h

OUTPUT_PATH     = "C:/traffic-project/output"
CHECKPOINT_PATH = "C:/traffic-project/checkpoint"

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("TrafficCongestionDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── JSON SCHEMA ───────────────────────────────────────────────────────────────
message_schema = StructType([
    StructField("location",         StringType(),  True),
    StructField("vehicles",         IntegerType(), True),
    StructField("congestion_level", StringType(),  True),
    StructField("current_speed",    DoubleType(),  True),
    StructField("freeflow_speed",   DoubleType(),  True),
    StructField("ratio",            DoubleType(),  True),
    StructField("confidence",       DoubleType(),  True),
    StructField("timestamp",        StringType(),  True),
])

# ── READ FROM KAFKA ────────────────────────────────────────────────────────────
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic") \
    .load()

# ── PARSE JSON ────────────────────────────────────────────────────────────────
parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), message_schema)) \
    .select(
        col("data.location").alias("location"),
        col("data.vehicles").alias("vehicles"),
        col("data.current_speed").alias("current_speed"),
        col("data.freeflow_speed").alias("freeflow_speed"),
        col("data.ratio").alias("ratio"),
        col("data.confidence").alias("confidence"),
        col("data.timestamp").alias("timestamp"),
    )

# ── DROP BAD ROWS ─────────────────────────────────────────────────────────────
filtered_df = parsed_df.filter(
    col("location").isNotNull() &
    col("vehicles").isNotNull() &
    col("ratio").isNotNull() &
    (col("location") != "") &
    (col("vehicles") > 0)
)

# ── CLASSIFY using combined ratio + absolute speed ────────────────────────────
result_df = filtered_df.withColumn(
    "congestion_level",
    when(
        (col("current_speed") < ABS_HIGH) | (col("ratio") < RATIO_HIGH),
        "HIGH"
    ).when(
        (col("current_speed") < ABS_MEDIUM) | (col("ratio") < RATIO_MEDIUM),
        "MEDIUM"
    ).otherwise("LOW")
)

# ── FINAL COLUMNS ─────────────────────────────────────────────────────────────
output_df = result_df.select(
    "location",
    "vehicles",
    "congestion_level",
    "current_speed",
    "freeflow_speed",
    "ratio",
    "confidence",
    "timestamp",
)

# ── WRITE CSV ─────────────────────────────────────────────────────────────────
query = output_df.writeStream \
    .format("csv") \
    .option("path", OUTPUT_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("header", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()