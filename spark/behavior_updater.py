from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as sum_, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
import redis

spark = SparkSession.builder.appName("behavior_updater").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "1")

TX_SCHEMA = StructType([
    StructField("account_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("event_time", TimestampType())
])

df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9094")
      .option("subscribe", "transactions_clean")
      .load()
      .select(from_json(col("value").cast("string"), TX_SCHEMA).alias("tx"))
      .select("tx.*")
      .withWatermark("event_time", "10 minutes"))


# --- TÁCH RIÊNG WINDOW 1 PHÚT (Tumbling Window) ---
stats_1m = (df.groupBy(col("account_id"), window(col("event_time"), "1 minute"))
    .agg(count("*").alias("cnt_1m")))

# --- TÁCH RIÊNG WINDOW 5 PHÚT (Sliding Window) ---
stats_5m = (df.groupBy(col("account_id"), window(col("event_time"), "5 minutes", "1 minute"))
    .agg(
        sum_(when(col("status") == "SUCCESS", col("amount")).otherwise(0)).alias("sum_5m"),
        count(when(col("status") == "FAILED", 1)).alias("fail_5m")
    ))

def update_redis_combined(batch_df, batch_id):
    # Lưu ý: batch_df ở đây sẽ chứa kết quả từ cả stats_1m hoặc stats_5m 
    # tùy vào micro-batch nào của Spark vừa hoàn thành.
    
    def process_partition(partition):
        r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        pipe = r.pipeline()
        for row in partition:
            key = f"bhv:{row.account_id}"
            
            # Kiểm tra xem row này thuộc về Window 1m hay 5m bằng cách kiểm tra các cột
            if "cnt_1m" in row.asDict():
                pipe.hset(key, "tx_count_1m", row.cnt_1m)
            else:
                pipe.hset(key, mapping={
                    "amount_sum_5m": row.sum_5m,
                    "failed_tx_5m": row.fail_5m
                })
            pipe.expire(key, 600)
        pipe.execute()

    batch_df.foreachPartition(process_partition)

# Ghi cả 2 luồng tính toán vào Redis
query_1m = stats_1m.writeStream.foreachBatch(update_redis_combined).outputMode("update").start()
query_5m = stats_5m.writeStream.foreachBatch(update_redis_combined).outputMode("update").start()

spark.streams.awaitAnyTermination()