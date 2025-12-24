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


-- =============================== Bảng login_logs ===============================
CREATE TABLE login_logs (
    log_id       BIGSERIAL PRIMARY KEY,
    customer_id  INT REFERENCES customer(customer_id),
    ip_address   VARCHAR(50),
    location     VARCHAR(100),
    device_id    VARCHAR(255),
    login_time   TIMESTAMP DEFAULT NOW(),
    success      BOOLEAN
);
-- Bạn enrich transaction stream bằng lịch sử đăng nhập
-- Giúp detect fraud:
-- login from new device
-- login from suspicious location
-- nhiều lần login fail


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

INSERT INTO customer (full_name, date_of_birth, phone, email, address, risk_level)
VALUES
('Pham Ba Trung', '1999-11-09', '0935251234', '1109trungpham@example.com', 'Hue', 'LOW'),
('Phan Thi Dieu Linh', '1999-08-30', '0909111113', 'linhphan3008@example.com', 'HCMC', 'MEDIUM'),
('Pham Tuam', '1999-11-05', '0909111111', 'erictuan511@example.com', 'Da Nang', 'LOW'),
('Ho Nhu Ngoc', '1999-11-30', '0909111112', 'nhungoc3011@example.com', 'Ha Noi', 'HIGH');


INSERT INTO account (customer_id, account_type, balance, status)
VALUES
(1, 'SAVINGS', 21000, 'ACTIVE'),
(1, 'WALLET', 5000, 'ACTIVE'),
(2, 'SAVINGS', 35000, 'ACTIVE'),
(2, 'CARD', 10000, 'ACTIVE'),
(3, 'SAVINGS', 40000, 'ACTIVE'),
(4, 'CARD', 15000, 'ACTIVE');


INSERT INTO login_logs (customer_id, ip_address, location, device_id, success)
VALUES
-- User 1: hành vi bình thường
(1, '113.23.44.12', 'Hue', 'DEVICE_A1', true),
(1, '113.23.44.12', 'Hue', 'DEVICE_A1', true),
-- User 2: đăng nhập từ IP lạ
(2, '52.12.99.44', 'HCMC', 'DEVICE_B1', true),
(2, '52.12.99.44', 'HCMC', 'DEVICE_B1', false),
(2, '14.162.22.33', 'HCMC', 'DEVICE_B1', true),
-- User 3: đăng nhập từ thiết bị mới
(3, '113.18.44.99', 'Da Nang', 'DEVICE_C1', true),
(3, '192.168.1.88', 'Unknown', 'NEW_DEVICE_X', true),
-- User 4: nhiều lần login fail
(4, '113.55.12.77', 'Ha Noi', 'DEVICE_D1', false),
(4, '113.55.12.77', 'Ha Noi', 'DEVICE_D1', false),
(4, '113.55.12.77', 'Ha Noi', 'DEVICE_D1', true);



INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(1, 500, 'VND', 'PAYMENT', 'Shopee', 'DEVICE_A1', '113.23.44.12', 'Hue', 'SUCCESS'),
(1, 1000, 'VND', 'TRANSFER', 'MB Bank', 'DEVICE_A1', '113.23.44.12', 'Hue', 'SUCCESS');


-- Case 1 — Amount Spike (giao dịch lớn bất thường)
INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(2, 95000000, 'VND', 'TRANSFER', 'Techcombank', 'DEVICE_B1', '14.162.22.33', 'HCMC', 'SUCCESS');


-- Case 2 — Location Jump (đăng nhập tại HCMC nhưng giao dịch ở Singapore)
INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(2, 7000000, 'VND', 'PAYMENT', 'Grab SG', 'DEVICE_B1', '52.12.99.44', 'Singapore', 'SUCCESS');


-- Case 3 — New Device Transaction (user 3 dùng device lạ)
INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(3, 3500000, 'VND', 'PAYMENT', 'Tiki', 'NEW_DEVICE_X', '192.168.1.88', 'Unknown', 'SUCCESS');


-- Case 4 — Account with high-risk KYC → giao dịch lớn
INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(6, 12000000, 'VND', 'WITHDRAW', 'ATM BIDV', 'DEVICE_D1', '113.55.12.77', 'Hai Phong', 'SUCCESS');
