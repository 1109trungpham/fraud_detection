from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import *
import json

spark = (
    SparkSession.builder
        .appName("StateBuilderSimple")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ======================
# Debezium payload schema
# ======================
schema_payload = StructType([
    StructField("before", MapType(StringType(), StringType()), True),
    StructField("after", MapType(StringType(), StringType()), True),
    StructField("op", StringType(), True),
])

# Function to extract "after"
def extract_after(df):
    return (
        df
        .withColumn("after_json", expr("CAST(value AS STRING)"))
        .withColumn("payload", from_json("after_json", StructType([
            StructField("payload", schema_payload)
        ])))
        .selectExpr("payload.payload.after as data", "payload.payload.op as op")
        .where("data IS NOT NULL")
    )

# ======================
# Read raw streams
# ======================

topic_customer = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "banking_topic_2.public.customer") \
    .load()

topic_account = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "banking_topic_2.public.account") \
    .load()

topic_login = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "banking_topic_2.public.login_logs") \
    .load()

topic_tx = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9094") \
    .option("subscribe", "banking_topic_2.public.transactions") \
    .load()

# Parse Debezium
df_customer = extract_after(topic_customer)
df_account = extract_after(topic_account)
df_login = extract_after(topic_login)
df_tx = extract_after(topic_tx)

# ======================
# OUTPUT: Save raw state
# ======================

query1 = df_customer.writeStream \
    .format("json") \
    .option("path", "/opt/spark/state/customer") \
    .option("checkpointLocation", "/opt/spark/checkpoints/customer") \
    .outputMode("append") \
    .start()

query2 = df_account.writeStream \
    .format("json") \
    .option("path", "/opt/spark/state/account") \
    .option("checkpointLocation", "/opt/spark/checkpoints/account") \
    .outputMode("append") \
    .start()

query3 = df_login.writeStream \
    .format("json") \
    .option("path", "/opt/spark/state/login_logs") \
    .option("checkpointLocation", "/opt/spark/checkpoints/login_logs") \
    .outputMode("append") \
    .start()

query4 = df_tx.writeStream \
    .format("json") \
    .option("path", "/opt/spark/state/transactions") \
    .option("checkpointLocation", "/opt/spark/checkpoints/transactions") \
    .outputMode("append") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
