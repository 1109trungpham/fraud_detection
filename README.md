

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  --data '@debezium-postgres-connector.json' http://localhost:8083/connectors

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
VALUES (101, 5000, 'transfer', 'Hanoi'), (102, 95000000, 'transfer', 'HaiPhong');

docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/fraud_stream.py


