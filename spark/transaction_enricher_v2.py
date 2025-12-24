from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
)
from pyspark.sql.functions import col, from_json, to_json, struct
import redis
import json
from pyspark.sql import Row
from datetime import datetime

spark = SparkSession.builder.appName("Transaction_Enricher_Final").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "1")

# 1. Schema gốc (Input từ transactions_clean)
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
    StructField("status", StringType()), # Sẽ đổi thành tx_status
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("event_date", StringType()),
    StructField("ingest_time", TimestampType()),
    StructField("is_deleted", BooleanType()) # Sẽ đổi thành tx_is_deleted
    # Các trường lsn, table... sẽ bị loại bỏ ở output cuối
])

# 2. Schema đầu ra CHUẨN (Khớp 100% mẫu yêu cầu)
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
    StructField("event_time", StringType()), # Dạng chuỗi ISO
    StructField("event_date", StringType()),
    StructField("ingest_time", StringType()),
    StructField("tx_is_deleted", BooleanType()),
    StructField("customer_id", IntegerType()),
    StructField("account_type", StringType()),
    StructField("account_status", StringType()),
    StructField("account_is_deleted", BooleanType()),
    StructField("account_updated_at", StringType()),
    StructField("customer_risk", StringType()),
    StructField("customer_updated_at", StringType()),
    StructField("enriched_at", StringType())
])

def enrich_with_redis_pipeline(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    def process_partition(partition):
        # Kết nối Redis với decode_responses=True để nhận string thay vì bytes
        r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        results = []
        records = list(partition)
        if not records: return iter([])

        # --- BƯỚC 1: LOOKUP ACCOUNT ---
        pipe_acc = r.pipeline()
        for row in records:
            pipe_acc.get(f"acc:{row.account_id}")
        accounts_data = pipe_acc.execute()
        
        # --- BƯỚC 2: LOOKUP CUSTOMER ---
        parsed_accounts = []
        customer_keys = []
        for acc_json in accounts_data:
            acc_obj = json.loads(acc_json) if acc_json else None
            parsed_accounts.append(acc_obj)
            if acc_obj and acc_obj.get("customer_id"):
                customer_keys.append(f"cus:{acc_obj.get('customer_id')}")
            else:
                customer_keys.append(None)
        
        pipe_cus = r.pipeline()
        for c_key in customer_keys:
            if c_key: pipe_cus.get(c_key)
            else: pipe_cus.get("non_existent_key")
        customers_data = pipe_cus.execute()
        
        # --- BƯỚC 3: MAPPING TO FINAL JSON ---
        now_str = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        for i in range(len(records)):
            tx = records[i]
            acc = parsed_accounts[i]
            cus = json.loads(customers_data[i]) if customers_data[i] else None
            
            # Tạo dictionary mới với key đặt tên lại theo yêu cầu
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
                "tx_status": tx.status, # Đổi tên
                "event_type": tx.event_type,
                "event_time": tx.event_time.isoformat() if tx.event_time else None,
                "event_date": tx.event_date,
                "ingest_time": tx.ingest_time.isoformat() if tx.ingest_time else None,
                "tx_is_deleted": tx.is_deleted, # Đổi tên
                
                # Thông tin từ Account Redis
                "customer_id": acc.get("customer_id") if acc else None,
                "account_type": acc.get("account_type") if acc else None,
                "account_status": acc.get("status") if acc else None,
                "account_is_deleted": acc.get("is_deleted") if acc else None,
                "account_updated_at": acc.get("updated_at") if acc else None,
                
                # Thông tin từ Customer Redis
                "customer_risk": cus.get("risk_level") if cus else "UNKNOWN",
                "customer_updated_at": cus.get("updated_at") if cus else None,
                
                "enriched_at": now_str
            }
            
            # Đảm bảo thứ tự Row khớp với ENRICHED_SCHEMA
            results.append(Row(*[out.get(f.name) for f in ENRICHED_SCHEMA]))
            
        return iter(results)

    enriched_rdd = batch_df.rdd.mapPartitions(process_partition)
    enriched_df = spark.createDataFrame(enriched_rdd, schema=ENRICHED_SCHEMA)
    
    (enriched_df
        .selectExpr("CAST(tx_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9094")
        .option("topic", "transactions_enriched")
        .save())

# 3. Read Stream
df_tx = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9094")
    .option("subscribe", "transactions_clean")
    .load()
    .select(from_json(col("value").cast("string"), tx_schema).alias("tx"))
    .select("tx.*")
)

query = (
    df_tx.writeStream
    .foreachBatch(enrich_with_redis_pipeline)
    .option("checkpointLocation", "/opt/spark/checkpoints/tx_enriched_redis")
    .start()
)

query.awaitTermination()