from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.sql.functions import when, col, from_json, to_timestamp, from_unixtime

spark = SparkSession.builder.appName("ReadTransactions").getOrCreate()


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
    StructField("op", StringType(), True)
])

debezium_schema = StructType([
    StructField("schema", StringType(), True),   # không dùng
    StructField("payload", payload_schema, True)
])



df_kafka = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9094")
        .option("subscribe", "banking_topic_2.public.transactions")
        .option("startingOffsets", "latest")
        .load()
)

df_parsed = (
    df_kafka
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
            "transaction",
            when(col("op") == "d", col("before"))  # delete → dùng before
            .otherwise(col("after"))               # insert/update → after
        )
        .select(
            "event_type",
            col("transaction.*")  # unfold
        )
)


clean_tx = df_events \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("created_at_ts", to_timestamp( from_unixtime(col("created_at") / 1_000_000) ))

query = (
    clean_tx.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
)

query.awaitTermination()

