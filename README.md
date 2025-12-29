
# ===================== Phase 5 (Behavior Enrichment + Rule Engine) =====================

```bash
# Khởi động hệ thống
docker compose up -d
```

```bash
docker exec -it banking_postgres psql -U postgres -d banking
# -> Chạy lệnh tạo các bảng (postgres/db.sql)
```


```bash
# Tạo connect:
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector.json

# Kiểm tra trạng thái connect (output mong đợi: RUNNING - RUNNING):
curl localhost:8083/connectors/cdc-banking-postgres/status | jq

# Xoá connect:
curl -X DELETE http://localhost:8083/connectors/cdc-banking-postgres
```


```bash
# Tạo topic kafka:
docker exec -it kafka kafka-topics --create --topic cdc.banking.public.customer --bootstrap-server kafka:9094 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic cdc.banking.public.account --bootstrap-server kafka:9094 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic cdc.banking.public.transactions --bootstrap-server kafka:9094 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic transactions_clean --bootstrap-server kafka:9094 --partitions 3 --replication-factor 1

docker exec -it kafka kafka-topics --create --topic transactions_enriched --bootstrap-server kafka:9094 --partitions 3 --replication-factor 1

# Xem danh sách topic:
docker exec -it kafka kafka-topics --bootstrap-server kafka:9094 --list

# __consumer_offsets
# cdc.banking.public.account
# cdc.banking.public.customer
# cdc.banking.public.transactions
# connect-configs
# connect-offsets
# connect-status
# transactions_clean
# transactions_enriched
```

```bash
# Chạy các Spark Job:
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/state_builder_customer.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/state_builder_account.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/transaction_clean.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/behavior_updater.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/transaction_enricher.py
```

```bash
docker exec -it banking_postgres psql -U postgres -d banking
# -> Chạy lệnh chèn dữ liệu cho bảng customer và account (postgres/db.sql)
```

```bash
# Quan sát giao dịch sau khi đã làm giàu thông tin:
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9094 --topic transactions_enriched
```

```bash
# Khởi động công cụ giám sát:
python3 spark/fraud_rule_engine.py

# -> Test hệ thống (postgres/db.sql)
```