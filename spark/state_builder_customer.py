from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               LongType, IntegerType, StringType)
from pyspark.sql.functions import col, when, from_json, to_json, struct, lit, current_timestamp, to_timestamp

spark = SparkSession.builder.appName("State_Accounts").getOrCreate()


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

payload_schema = StructType([
    StructField("before", customer_schema, True),
    StructField("after", customer_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

debezium_schema = StructType([
    StructField("schema", StringType(), True),  # không dùng
    StructField("payload", payload_schema, True)
])

df_customer_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "cdc.banking.public.customer")
    .option("startingOffsets", "latest")
    .load()
)

df_customer = (
    df_customer_raw.select(from_json(col("value").cast("string"), debezium_schema).alias("data"))
      .select("data.payload.*")
)

df_customer_events = (
    df_customer
    .withColumn("record", when(col("op") == "d", col("before")).otherwise(col("after")))
    .withColumn("is_deleted", col("op") == "d")
)

df_customer_state = (
    df_customer_events
    .filter(col("record.customer_id").isNotNull())
    .select(
        col("record.customer_id").cast("string").alias("key"),
        to_json(
            struct(
                col("record.customer_id"),
                col("record.risk_level"),
                col("is_deleted"),
                to_timestamp(col("ts_ms") / 1000).alias("updated_at")
            )
        ).alias("value")
    )
)

query = df_customer_state.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("topic", "customer_dim_state") \
    .option("checkpointLocation", "/opt/spark/checkpoints/customer_dim") \
    .start()

query.awaitTermination()