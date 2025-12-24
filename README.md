
# ===================== Phase 1 =====================

```bash
docker compose up -d
docker exec -it banking_postgres psql -U postgres -d banking
```

```sql
CREATE TABLE bank_transactions (
    id SERIAL PRIMARY KEY,
    account_id INT NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    transaction_type VARCHAR(20),
    location VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE bank_transactions
ALTER COLUMN amount TYPE BIGINT;
```

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector.json

curl localhost:8083/connectors/postgres-banking-connector/status | jq

curl -X DELETE http://localhost:8083/connectors/postgres-banking-connector
```

```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9094 --topic banking_topic.public.bank_transactions --from-beginning
```

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/fraud_stream.py
```

```sql
INSERT INTO bank_transactions(account_id, amount, transaction_type, location)
VALUES (101, 75000000, 'transfer', 'Hanoi');

INSERT INTO bank_transactions(account_id, amount, transaction_type, location)
VALUES (101, 50000, 'transfer', 'Hue'), (102, 15000000, 'transfer', 'DaNang');
```

# ===================== Phase 2 =====================

```bash
curl -o postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
mv postgresql.jar jars/postgresql.jar
```

```bash
docker compose up -d
docker exec -it banking_postgres psql -U postgres -d banking
# -> chạy các lệnh tạo bảng (trong postgres/db.sql)
```

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector-p2.json

curl localhost:8083/connectors/postgres-banking-connector-2/status | jq

curl -X DELETE http://localhost:8083/connectors/postgres-banking-connector-2
```

```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9094 --topic banking_topic_2.public.transactions --from-beginning
```

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/read_transactions.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/stream_state_builder2.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/main_streaming.py
```

# ===================== Phase 3 (start over) =====================

### Tải jar postgres
```bash
curl -o postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
mv postgresql.jar jars/postgresql.jar
```

### Khởi động hệ thống
```bash
docker compose up -d
```

### Khởi tạo cơ sở dữ liệu
```bash
docker exec -it banking_postgres psql -U postgres -d banking
# -> chạy các lệnh tạo bảng và insert dữ liệu trong postgres/db.sql
```

### Tạo connect
```bash
# Lệnh tạo connect
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector-p3.json

# Lệnh kiểm tra trạng thái connect:
curl localhost:8083/connectors/cdc-banking-postgres/status | jq
# Output mong đợi: RUNNING - RUNNING

# Lệnh xoá connect:
curl -X DELETE http://localhost:8083/connectors/cdc-banking-postgres
```

Sau khi tạo connect, các kafka topics được tạo ra:
```
cdc.banking.public.transactions
cdc.banking.public.customer
cdc.banking.public.account
cdc.banking.public.login_logs
```

### Tương tác với Kafka:
```bash
docker exec -it kafka bash

kafka-topics --bootstrap-server kafka:9094 --list

kafka-console-consumer --bootstrap-server kafka:9094 --topic cdc.banking.public.transactions --from-beginning

kafka-console-consumer --bootstrap-server kafka:9094 --topic transactions_clean --from-beginning
```

### Tương tác với Spark:
```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/spark_parser.py
```

### Test
```sql
INSERT INTO transactions (account_id, amount, currency, tx_type, merchant, device_id, ip_address, location, status)
VALUES (1, 1500000, 'VND', 'PAYMENT', 'Shopee', 'DEVICE_A1', '113.23.44.12', 'Hanoi', 'SUCCESS');
```


# ===================== Phase 4 (Enrichment) =====================

```bash
docker exec -it banking_postgres psql -U postgres -d banking
```
```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9094 --topic customer_dim_state --from-beginning
kafka-console-consumer --bootstrap-server kafka:9094 --topic account_dim_state --from-beginning
```
```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/state_builder_customer.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/state_builder_account.py
```


```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9094 --topic transactions_enriched --from-beginning
```

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/transaction_enricher.py
```

```sql
INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(15, 4000, 'VND', 'TRANSFER', 'MB Bank', 'DEVICE_A1', '113.23.44.12', 'Hue', 'SUCCESS');
```