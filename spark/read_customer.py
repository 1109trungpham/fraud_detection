from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, from_json

spark = SparkSession.builder.appName("ReadCustomer").getOrCreate()

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StringType(), True),
    StructField("risk_level", StringType(), True)
])

payload_schema = StructType([
    StructField("before", customer_schema, True),
    StructField("after", customer_schema, True),
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
    .option("subscribe", "banking_topic_2.public.customer")
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
)

query = (
    df_events.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .start()
)

query.awaitTermination()
