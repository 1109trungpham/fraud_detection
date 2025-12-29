from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from pyspark.sql.functions import col, when, from_json, to_json, struct, lit, to_timestamp
import redis

spark = SparkSession.builder.appName("state_accounts").getOrCreate()


account_schema = StructType([
    StructField("account_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("account_type", StringType(), True),
    StructField("balance", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

payload_schema = StructType([
    StructField("before", account_schema, True),
    StructField("after", account_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

debezium_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])

df_account_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "cdc.banking.public.account")
    .option("startingOffsets", "latest")
    .load()
)

df_account = (
    df_account_raw.select(from_json(col("value").cast("string"), debezium_schema).alias("data"))
      .select("data.payload.*")
)

df_account_events = (
    df_account
    .withColumn("record", when(col("op") == "d", col("before")).otherwise(col("after")))
    .withColumn("is_deleted", col("op") == "d")
)

df_account_state = (
    df_account_events
    .filter(col("record.account_id").isNotNull())
    .select(
        col("record.account_id").cast("string").alias("key"),
        to_json(
            struct(
                col("record.account_id"),
                col("record.customer_id"),
                col("record.account_type"),
                when(col("op") == "d", lit("DELETED")).otherwise(col("record.status")).alias("status"),
                col("is_deleted"),
                to_timestamp(col("ts_ms") / 1000).alias("updated_at")
            )
        ).alias("value")
    )
)


def write_to_kafka_and_redis(batch_df, batch_id):
    # 1. Ghi vào Kafka (Source of Truth)
    batch_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9094") \
        .option("topic", "account_dim_state") \
        .save()

    # 2. Ghi vào Redis (High-speed Lookup)
    def send_to_redis(partition):
        # Kết nối Redis tại mỗi Partition để tối ưu
        r = redis.Redis(host='redis', port=6379, db=0)
        pipe = r.pipeline()
        for row in partition:
            # Lưu key theo format "acc:{id}"
            pipe.set(f"acc:{row.key}", row.value)
        pipe.execute()

    batch_df.foreachPartition(send_to_redis)


query = df_account_state.writeStream \
    .foreachBatch(write_to_kafka_and_redis) \
    .option("checkpointLocation", "/opt/spark/checkpoints/account_dim") \
    .start()

query.awaitTermination()