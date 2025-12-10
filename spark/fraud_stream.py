from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()


transaction_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("amount", LongType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", LongType(), True)  # MicroTimestamp
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
        .option("subscribe", "banking_topic.public.bank_transactions")
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



# Rule: giao dịch > 50 triệu → nghi vấn
fraud_df = df_events.filter(col("amount") > 50_000_000)

query = (
    df_events.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
)

query.awaitTermination()

