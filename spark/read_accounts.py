from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, from_json, from_unixtime, to_timestamp

spark = SparkSession.builder.appName("ReadAccounts").getOrCreate()

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
    StructField("op", StringType(), True)
])

debezium_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", payload_schema, True)
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "banking_topic_2.public.account")
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df.select(from_json(col("value").cast("string"), debezium_schema).alias("data"))
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
    .withColumn("record",
        when(col("op") == "d", col("before")).otherwise(col("after"))
    )
    .select("event_type", col("record.*"))
    .withColumn("balance", col("balance").cast("double"))
    .withColumn("updated_at_ts",
        to_timestamp(from_unixtime(col("updated_at") / 1_000_000))
    )
)

query = (
    df_events.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
)

query.awaitTermination()
