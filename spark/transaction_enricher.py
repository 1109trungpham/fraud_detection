from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, from_json
import redis
import json
from pyspark.sql import Row
from datetime import datetime

spark = SparkSession.builder.appName("Transaction_Dimension+Bihavior_Enrichment").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "1")

# 1. Schema gốc (Input từ transactions_clean)
TX_SCHEMA = StructType([
    StructField("tx_id", IntegerType()),
    StructField("account_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("tx_type", StringType()),
    StructField("merchant", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_address", StringType()),
    StructField("location", StringType()),
    StructField("status", StringType()), # Sẽ đổi thành tx_status
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("event_date", StringType()),
    StructField("ingest_time", TimestampType()),
    StructField("is_deleted", BooleanType()) # Sẽ đổi thành tx_is_deleted
    # Các trường lsn, table... sẽ bị loại bỏ ở output cuối
])

# 2. Schema đầu ra CHUẨN
ENRICHED_SCHEMA = StructType([
    StructField("tx_id", IntegerType()),
    StructField("account_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("tx_type", StringType()),
    StructField("merchant", StringType()),
    StructField("device_id", StringType()),
    StructField("ip_address", StringType()),
    StructField("location", StringType()),
    StructField("tx_status", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("event_date", StringType()),
    StructField("ingest_time", StringType()),
    StructField("tx_is_deleted", BooleanType()),

    # ---- Dimension ----
    StructField("customer_id", IntegerType()),
    StructField("account_type", StringType()),
    StructField("account_status", StringType()),
    StructField("account_is_deleted", BooleanType()),
    StructField("account_updated_at", StringType()),
    StructField("customer_risk", StringType()),
    StructField("customer_updated_at", StringType()),

    # ---- Behavior ----
    StructField("tx_count_1m", IntegerType()),
    StructField("amount_sum_5m", DoubleType()),
    StructField("failed_tx_5m", IntegerType()),
    StructField("last_location", StringType()),

    StructField("enriched_at", StringType())
])


def enrich_process(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    def process_partition(partition):
            
            r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
            results = []
            records = list(partition)

            if not records:
                return iter([])
            
            # --- BƯỚC 1: PIPELINE LOOKUP ACCOUNT, BEHAVIOR VÀ LOCATION ---
            pipe1 = r.pipeline()
            for row in records:
                pipe1.get(f"acc:{row.account_id}")     # Để lấy customer_id
                pipe1.hgetall(f"bhv:{row.account_id}") # Behavior Stats
                pipe1.get(f"loc:{row.account_id}")     # Last Location
            res1 = pipe1.execute()

            # Phân tách kết quả từ res1 và chuẩn bị keys cho Customer Lookup
            parsed_accounts = []
            behaviors = []
            last_locations = []
            customer_keys = []
            
            for i in range(len(records)):
                idx = i * 3
                acc_obj = json.loads(res1[idx]) if res1[idx] else {}

                parsed_accounts.append(acc_obj)
                behaviors.append(res1[idx+1] if res1[idx+1] else {})
                last_locations.append(res1[idx+2])
                
                # Lấy customer_id từ account để tạo key lookup customer
                cus_id = acc_obj.get("customer_id")
                customer_keys.append(f"cus:{cus_id}" if cus_id else None)

            # --- BƯỚC 2: PIPELINE LOOKUP CUSTOMER ---
            pipe2 = r.pipeline()
            for c_key in customer_keys:
                if c_key:
                    pipe2.get(c_key)
                else:
                    # Dùng một key không tồn tại để giữ đúng thứ tự index trong pipeline kết quả
                    pipe2.get("non_existent_key") 
            customers_data = pipe2.execute()

            # --- BƯỚC 3: UPDATE LOCATION (SIDE EFFECT) ---
            pipe_upd = r.pipeline()
            for row in records:
                pipe_upd.set(f"loc:{row.account_id}", row.location)
            pipe_upd.execute()

            # --- BƯỚC 4: MERGE TẤT CẢ DỮ LIỆU ---
            now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            for i in range(len(records)):
                tx = records[i]
                acc = parsed_accounts[i]
                cus = json.loads(customers_data[i]) if customers_data[i] else {}
                bhv = behaviors[i]
                last_loc = last_locations[i]

                out = {
                    "tx_id": tx.tx_id,
                    "account_id": tx.account_id,
                    "amount": tx.amount,
                    "currency": tx.currency,
                    "tx_type": tx.tx_type,
                    "merchant": tx.merchant,
                    "device_id": tx.device_id,
                    "ip_address": tx.ip_address,
                    "location": tx.location,
                    "tx_status": tx.status,
                    "event_type": tx.event_type,
                    "event_time": tx.event_time.isoformat() if tx.event_time else None,
                    "event_date": tx.event_date,
                    "ingest_time": tx.ingest_time.isoformat() if tx.ingest_time else None,
                    "tx_is_deleted": tx.is_deleted,
                    
                    # Thông tin từ Account
                    "customer_id": acc.get("customer_id"),
                    "account_type": acc.get("account_type"),
                    "account_status": acc.get("status"),
                    "account_is_deleted": acc.get("is_deleted"),
                    "account_updated_at": acc.get("updated_at"),
                    
                    # Thông tin từ Customer
                    "customer_risk": cus.get("risk_level", "UNKNOWN"),
                    "customer_updated_at": cus.get("updated_at"),
                    
                    # Thông tin Behavior
                    "tx_count_1m": int(bhv.get("tx_count_1m", 0)),
                    "amount_sum_5m": float(bhv.get("amount_sum_5m", 0.0)),
                    "failed_tx_5m": int(bhv.get("failed_tx_5m", 0)),
                    "last_location": last_loc,
                    "enriched_at": now_str
                }
                results.append(Row(*[out.get(f.name) for f in ENRICHED_SCHEMA]))
                
            return iter(results)

    enriched_df = spark.createDataFrame(batch_df.rdd.mapPartitions(process_partition), schema=ENRICHED_SCHEMA)
    
    (enriched_df.selectExpr("CAST(tx_id AS STRING) AS key", "to_json(struct(*)) AS value")
     .write.format("kafka").option("kafka.bootstrap.servers", "kafka:9094")
     .option("topic", "transactions_enriched").save())

# Read Stream
df_tx = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9094")
         .option("subscribe", "transactions_clean")
         .load()
         .select(from_json(col("value").cast("string"), TX_SCHEMA).alias("tx")).select("tx.*"))

query = (df_tx.writeStream.foreachBatch(enrich_process)
         .option("checkpointLocation", "/opt/spark/checkpoints/tx_enriched_redis")
         .start())

query.awaitTermination()