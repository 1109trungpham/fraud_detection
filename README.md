

docker compose up -d

docker exec -it banking_postgres psql -U postgres -d banking
<!--
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

-->


curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector.json

curl localhost:8083/connectors/postgres-banking-connector/status | jq

curl -X DELETE http://localhost:8083/connectors/postgres-banking-connector


docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9094 --topic banking_topic.public.bank_transactions --from-beginning

INSERT INTO bank_transactions(account_id, amount, transaction_type, location)
VALUES (101, 75000000, 'transfer', 'Hanoi');

INSERT INTO bank_transactions(account_id, amount, transaction_type, location)
VALUES (101, 50000, 'transfer', 'Hue'), (102, 15000000, 'transfer', 'DaNang');

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/fraud_stream.py



# Phase 2

docker compose up -d

docker exec -it banking_postgres psql -U postgres -d banking

chạy các lệnh tạo bảng (trong postgres/db.sql)


curl -o postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
mv postgresql.jar jars/postgresql.jar

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-postgres-connector-p2.json

curl localhost:8083/connectors/postgres-banking-connector-2/status | jq

curl -X DELETE http://localhost:8083/connectors/postgres-banking-connector-2


docker exec -it kafka bash
kafka-console-consumer --bootstrap-server kafka:9094 --topic banking_topic_2.public.transactions --from-beginning
kafka-console-consumer --bootstrap-server kafka:9094 --topic banking_topic_2.public.customer --from-beginning
kafka-console-consumer --bootstrap-server kafka:9094 --topic banking_topic_2.public.login_logs --from-beginning

<!-- kafka-topics --bootstrap-server localhost:9092 --list | grep banking_topic_2 -->

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/fraud_stream_master.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/read_transactions.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/read_accounts.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/read_customer.py

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/stream_state_builder2.py


docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/main_streaming.py