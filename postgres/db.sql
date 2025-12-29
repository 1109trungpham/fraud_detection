-- =============================================================================
-- ================================== SCHEMA ===================================
-- =============================================================================

-- =============================== Bảng customer ===============================
CREATE TABLE customer (
    customer_id   SERIAL PRIMARY KEY,
    full_name     VARCHAR(255),
    date_of_birth DATE,
    phone         VARCHAR(20),
    email         VARCHAR(255),
    address       TEXT,
    risk_level    VARCHAR(20),   -- LOW, MEDIUM, HIGH
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
-- Spark dùng để enrich thông tin user trong pipeline realtime
-- Debezium CDC sẽ stream nếu KYC thay đổi (rủi ro thay đổi)


-- Trigger tự động cập nhật updated_at
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customer_update
BEFORE UPDATE ON customer
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();


-- =============================== Bảng account ===============================
CREATE TABLE account (
    account_id    SERIAL PRIMARY KEY,
    customer_id   INT REFERENCES customer(customer_id),
    account_type  VARCHAR(50),   -- SAVINGS / WALLET / CARD
    balance       NUMERIC(18,2),
    status        VARCHAR(20),   -- ACTIVE / FROZEN / CLOSED
    created_at    TIMESTAMP DEFAULT NOW(),
    updated_at    TIMESTAMP DEFAULT NOW()
);
-- Có trường balance bị update → test Debezium UPDATE event
-- Có status để mô phỏng hành động của anti-fraud service (freeze account)


CREATE TRIGGER trg_account_update
BEFORE UPDATE ON account
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();


-- =============================== Bảng transactions ===============================
CREATE TABLE transactions (
    tx_id          BIGSERIAL PRIMARY KEY,
    account_id     INT REFERENCES account(account_id),
    amount         NUMERIC(18,2),
    currency       VARCHAR(10),
    tx_type        VARCHAR(20),     -- TRANSFER / WITHDRAW / TOPUP / PAYMENT
    merchant       VARCHAR(255),
    device_id      VARCHAR(255),
    ip_address     VARCHAR(50),
    location       VARCHAR(100),
    status         VARCHAR(20),     -- PENDING / SUCCESS / FAILED
    created_at     TIMESTAMP DEFAULT NOW()
);
-- Đây là bảng mà Debezium cần stream liên tục → Spark xử lý realtime
-- Create nhiều giả lập giao dịch → pipeline chạy thật sự realtime
-- IP, location giúp bạn detect fraud:
-- Location jump
-- New device
-- Suspicious IP
-- Amount spikes


-- =============================================================================
-- =================================== DATA ====================================
-- =============================================================================

INSERT INTO customer (customer_id, full_name, date_of_birth, phone, email, address, risk_level)
VALUES
(1, 'Pham Ba Trung', '1999-11-09', '0935251234', '1109trungpham@example.com', 'Hue', 'LOW'),
(2, 'Phan Thi Dieu Linh', '1999-08-30', '0909111113', 'linhphan3008@example.com', 'HCMC', 'MEDIUM'),
(3, 'Pham Tuan', '1999-11-05', '0909111111', 'erictuan511@example.com', 'Da Nang', 'LOW'),
(4, 'Ho Nhu Ngoc', '1999-11-30', '0909111112', 'nhungoc3011@example.com', 'Ha Noi', 'LOW');


INSERT INTO account (customer_id, account_type, balance, status)
VALUES
(1, 'SAVINGS', 21000, 'ACTIVE'),
(1, 'WALLET', 5000, 'ACTIVE'),
(2, 'SAVINGS', 35000, 'ACTIVE'),
(2, 'CARD', 10000, 'ACTIVE'),
(3, 'SAVINGS', 40000, 'ACTIVE'),
(4, 'CARD', 15000, 'ACTIVE');


-- =============================================================================
-- ============================= TEST RULE ENGINE ==============================
-- =============================================================================

-- Case 1: Giao dịch bình thường
INSERT INTO transactions (account_id, amount, currency, tx_type, merchant, device_id, ip_address, location, status)
VALUES (2, 200, 'VND', 'TRANSFER', 'TechcomBank', 'DEVICE_A1', '113.23.44.12', 'Hue', 'SUCCESS');

-- Case 2: Giao dịch thất bại
INSERT INTO transactions (account_id, amount, currency, tx_type, merchant, device_id, ip_address, location, status)
VALUES (2, 100, 'VND', 'TRANSFER', 'TechcomBank', 'DEVICE_A1', '113.23.44.12', 'Hue', 'FAILED');

-- Case 3: Giao dịch với số tiền lớn (>50 triệu)
INSERT INTO transactions (account_id, amount, currency, tx_type, merchant, device_id, ip_address, location, status)
VALUES (2, 52000, 'VND', 'TRANSFER', 'TechcomBank', 'DEVICE_A1', '113.23.44.12', 'Hue', 'SUCCESS');

-- Case 4: Vị trí phát sinh giao dịch bất thường
INSERT INTO transactions (account_id, amount, currency, tx_type, merchant, device_id, ip_address, location, status)
VALUES (2, 340, 'VND', 'TRANSFER', 'UNKNOWN', 'DEVICE_X', '132.12.11.02', 'Dubai', 'SUCCESS');

-- Case 5: Thay đổi thông tin Khách hàng/Tài khoản
UPDATE customer
SET risk_level='HIGH'
WHERE customer_id=1;

UPDATE account
SET status='FROZEN'
WHERE account_id=2;

INSERT INTO transactions (account_id, amount, currency, tx_type, merchant, device_id, ip_address, location, status)
VALUES (2, 50, 'VND', 'TRANSFER', 'TechcomBank', 'DEVICE_A1', '113.23.44.12', 'Hue', 'SUCCESS');