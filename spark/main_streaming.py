from src.listeners.stream_listener import attach_listener
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, BooleanType, TimestampType
from pyspark.sql.functions import from_json, col, when

# =============================
# 1) Schema definitions
# =============================

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

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

account_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

login_logs_schema = StructType([
    StructField("log_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("ip_address", StringType(), True),
    StructField("location", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("login_time", StringType(), True),
    StructField("success", BooleanType(), True)
])





debezium_tx_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", StructType([
        StructField("before", transaction_schema, True),
        StructField("after", transaction_schema, True),
        StructField("op", StringType(), True)
    ]), True)
])

debezium_customer_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", StructType([
        StructField("before", customer_schema, True),
        StructField("after", customer_schema, True),
        StructField("op", StringType(), True)
    ]), True)
])

debezium_account_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", StructType([
        StructField("before", account_schema, True),
        StructField("after", account_schema, True),
        StructField("op", StringType(), True)
    ]), True)
])

debezium_login_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", StructType([
        StructField("before", login_logs_schema, True),
        StructField("after", login_logs_schema, True),
        StructField("op", StringType(), True)
    ]), True)
])








def build_cdc_stream(topic, schema):
    """
    Generic Debezium CDC reader
    """
    df_kafka = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9094")
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
    )

    df_parsed = (
        df_kafka
            .select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.payload.*")
    )

    df_events = (
        df_parsed
            .withColumn(
                "event_type",
                when(col("op") == "c", "INSERT")
                .when(col("op") == "u", "UPDATE")
                .when(col("op") == "d", "DELETE")
                .otherwise("UNKNOWN")
            )
            .withColumn(
                "record",
                when(col("op") == "d", col("before")).otherwise(col("after"))
            )
            .select("event_type", "record.*")
    )
    return df_events


# =============================
# 2) Build Spark session
# =============================
spark = (
    SparkSession.builder
    .appName("StreamingPipeline")
    .getOrCreate()
)

attach_listener(spark)


# =============================
# 3) Create streams for 4 topics
# =============================
df_tx = build_cdc_stream("banking_topic_2.public.transactions", debezium_tx_schema)
df_customer = build_cdc_stream("banking_topic_2.public.customer", debezium_customer_schema)
df_account = build_cdc_stream("banking_topic_2.public.account", debezium_account_schema)
df_login_logs = build_cdc_stream("banking_topic_2.public.login_logs", debezium_login_schema)


# =============================
# 4) Write state to storage
# =============================

def write_stream(df, name):
    return (
        df.writeStream
            .format("json")
            .option("path", f"/opt/spark/state/{name}")
            .option("checkpointLocation", f"/opt/spark/checkpoints/{name}")
            .outputMode("append")
            .start()
    )

q1 = write_stream(df_tx, "transactions")
q2 = write_stream(df_customer, "customer")
q3 = write_stream(df_account, "account")
q4 = write_stream(df_login_logs, "login_logs")

# =============================
# 5) Keep streaming job alive
# =============================
spark.streams.awaitAnyTermination()
