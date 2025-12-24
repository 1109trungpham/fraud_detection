from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               LongType, IntegerType, StringType)
from pyspark.sql.functions import when, col, lit, from_json, current_timestamp, to_date, to_timestamp, from_unixtime

spark = SparkSession.builder.appName("Spark_Clean").getOrCreate()


transaction_schema = StructType([
    StructField("tx_id", LongType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("tx_type", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True)
])

payload_schema = StructType([
    StructField("before", transaction_schema, True),
    StructField("after", transaction_schema, True),
    StructField("op", StringType(), True),
    StructField("source", StructType([
        StructField("table", StringType()),
        StructField("lsn", LongType())
    ]))
])

debezium_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])

# Read Kafka CDC
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9094")
        .option("subscribe", "cdc.banking.public.transactions")
        .option("startingOffsets", "latest")
        .load()
)

df_parsed = (
    df_raw
        .select(from_json(col("value").cast("string"), debezium_schema).alias("data"))
        .select("data.payload.*")
)

df_events = (
    df_parsed
        .withColumn(
            "event_type",
            when(col("op") == "c", "INSERT")
            .when(col("op") == "u", "UPDATE")
            .when(col("op") == "d", "DELETE")
            .when(col("op") == "r", "SNAPSHOT")
            .otherwise("UNKNOWN")
        )
        .withColumn(
            "record",
            when(col("op") == "d", col("before"))  # delete → dùng before
            .otherwise(col("after"))               # insert/update → after
        )
        .select(
            "event_type",
            col("source"),
            col("record.*")  # unfold
        )
)


# Normalize + Metadata
df_clean = (
    df_events
    .withColumn("amount", when(col("event_type") == "DELETE", lit(None)).otherwise(col("amount").cast("double")))
    .withColumn("event_time", to_timestamp( from_unixtime(col("created_at") / 1_000_000) ))
    .withColumn("ingest_time", current_timestamp())
    .withColumn("is_deleted", col("event_type") == "DELETE")
    .withColumn("cdc_source", lit("debezium"))
    .withColumn("table", col("source.table"))
    .withColumn("lsn", col("source.lsn"))
)


# Basic Validation (Streaming-safe, Option)
df_valid = df_clean.filter(
    col("tx_id").isNotNull() & # Luôn kiểm tra khóa chính
    (
        (col("event_type") == "DELETE") | # Nếu là DELETE, không cần kiểm tra các cột khác
        (col("account_id").isNotNull() & col("amount").isNotNull()) # Nếu là INSERT/UPDATE/SNAPSHOT, kiểm tra
    )
)

df_final = (
    df_valid
    .withColumn("event_date", to_date(col("event_time")))
    .select(
            # Thông tin giao dịch
            "tx_id", "account_id", "amount", "currency", "tx_type", 
            "merchant", "device_id", "ip_address", "location", "status",
            
            # Thông tin sự kiện/Thời gian
            "event_type", "event_time", "event_date", "ingest_time", "is_deleted",
            
            # Metadata CDC
            "cdc_source", "table", "lsn"
    )
)

# Write Kafka + Lake
query_kafka = df_final.selectExpr("CAST(tx_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("topic", "transactions_clean") \
    .option("checkpointLocation", "/opt/spark/checkpoints/tx_clean_kafka") \
    .start()


# Data lake (audit)
query_lake = df_final.writeStream.format("parquet") \
    .outputMode("append") \
    .option("path", "/lake/raw/transactions") \
    .option("checkpointLocation", "/opt/spark/checkpoints/tx_clean_lake") \
    .partitionBy("event_date") \
    .start()

spark.streams.awaitAnyTermination()