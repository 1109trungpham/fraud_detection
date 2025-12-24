from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, TimestampType
)
from pyspark.sql.functions import (
    col, from_json, to_json, struct,
    current_timestamp, broadcast
)

# =====================================================
# Spark Session
# =====================================================
spark = (
    SparkSession.builder
    .appName("Transaction_Enricher")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "1")

# =====================================================
# Schemas
# =====================================================

# ---------- transactions_clean ----------
tx_schema = StructType([
    StructField("tx_id", IntegerType()),
    StructField("account_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("tx_type", StringType()),
    StructField("merchant", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_address", StringType()),
    StructField("location", StringType()),
    StructField("status", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("event_date", StringType()),
    StructField("ingest_time", TimestampType()),
    StructField("is_deleted", BooleanType()),
    StructField("cdc_source", StringType()),
    StructField("table", StringType()),
    StructField("lsn", IntegerType())
])

# ---------- account_dim_state ----------
account_schema = StructType([
    StructField("account_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("account_type", StringType()),
    StructField("status", StringType()),
    StructField("is_deleted", BooleanType()),
    StructField("updated_at", TimestampType())
])

# ---------- customer_dim_state ----------
customer_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("risk_level", StringType()),
    StructField("updated_at", TimestampType())
])


# ---------- Transactions (Read Streams) ----------
df_tx = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "transactions_clean")
    .load()
    .select(from_json(col("value").cast("string"), tx_schema).alias("tx"))
    .select("tx.*")
)



# ---------- Account State (Read Table) ----------
df_account = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "account_dim_state")
    .load()
    .select(from_json(col("value").cast("string"), account_schema).alias("acc"))
    .select("acc.*")
)


# ---------- Customer State (Read Table) ----------
df_customer = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "customer_dim_state")
    .load()
    .select(from_json(col("value").cast("string"), customer_schema).alias("cus"))
    .select("cus.*")
)


# =====================================================
# Dimension Enrichment
# =====================================================

df_enriched = (
    df_tx.alias("tx")
    .join(broadcast(df_account).alias("acc"), "account_id", "left")
    .join(broadcast(df_customer).alias("cus"), "customer_id", "left")
)

# =====================================================
# Final Projection
# =====================================================

df_final = (
    df_enriched
    .select(
        # ================= TRANSACTION =================
        col("tx.tx_id"),
        col("tx.account_id"),
        col("tx.amount"),
        col("tx.currency"),
        col("tx.tx_type"),
        col("tx.merchant"),
        col("tx.device_id"),
        col("tx.ip_address"),
        col("tx.location"),
        col("tx.status").alias("tx_status"),
        col("tx.event_type"),
        col("tx.event_time"),
        col("tx.event_date"),
        col("tx.ingest_time"),
        col("tx.is_deleted").alias("tx_is_deleted"),

        # ================= ACCOUNT =================
        col("acc.customer_id"),
        col("acc.account_type"),
        col("acc.status").alias("account_status"),
        col("acc.is_deleted").alias("account_is_deleted"),
        col("acc.updated_at").alias("account_updated_at"),

        # ================= CUSTOMER =================
        col("cus.risk_level").alias("customer_risk"),
        col("cus.updated_at").alias("customer_updated_at"),

        # ================= META =================
        current_timestamp().alias("enriched_at")
    )
)


# =====================================================
# Write Kafka
# =====================================================

def write_to_kafka(batch_df, batch_id):
    (
        batch_df
        .selectExpr("CAST(tx_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9094")
        .option("topic", "transactions_enriched")
        .save()
    )

query = (
    df_final
    .writeStream
    .foreachBatch(write_to_kafka)
    .option("checkpointLocation", "/opt/spark/checkpoints/tx_enriched")
    .start()
)


query.awaitTermination()
