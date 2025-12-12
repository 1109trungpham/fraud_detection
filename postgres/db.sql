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



-- =============================================================================
-- =================================== DATA ====================================
-- =============================================================================

INSERT INTO customer (full_name, date_of_birth, phone, email, address, risk_level)
VALUES
('Nguyen Van A', '1990-05-12', '0901234567', 'a@example.com', 'Hanoi', 'LOW'),
('Tran Thi B', '1985-02-20', '0912345678', 'b@example.com', 'HCMC', 'MEDIUM'),
('Le Van C', '1998-09-15', '0931239999', 'c@example.com', 'Da Nang', 'LOW'),
('Pham Thi D', '1975-12-01', '0977778888', 'd@example.com', 'Hai Phong', 'HIGH');


INSERT INTO account (customer_id, account_type, balance, status)
VALUES
(1, 'SAVINGS', 12000000, 'ACTIVE'),
(1, 'WALLET', 500000, 'ACTIVE'),
(2, 'SAVINGS', 85000000, 'ACTIVE'),
(2, 'CARD', 2000000, 'ACTIVE'),
(3, 'SAVINGS', 15000000, 'ACTIVE'),
(4, 'CARD', 3000000, 'ACTIVE');


INSERT INTO login_logs (customer_id, ip_address, location, device_id, success)
VALUES
-- User 1: hành vi bình thường
(1, '113.23.44.12', 'Hanoi', 'DEVICE_A1', true),
(1, '113.23.44.12', 'Hanoi', 'DEVICE_A1', true),
-- User 2: đăng nhập từ IP lạ
(2, '52.12.99.44', 'Singapore', 'DEVICE_B1', true),
(2, '52.12.99.44', 'Singapore', 'DEVICE_B1', false),
(2, '14.162.22.33', 'HCMC', 'DEVICE_B1', true),
-- User 3: đăng nhập từ thiết bị mới
(3, '113.18.44.99', 'Da Nang', 'DEVICE_C1', true),
(3, '192.168.1.88', 'Unknown', 'NEW_DEVICE_X', true),
-- User 4: nhiều lần login fail
(4, '113.55.12.77', 'Hai Phong', 'DEVICE_D1', false),
(4, '113.55.12.77', 'Hai Phong', 'DEVICE_D1', false),
(4, '113.55.12.77', 'Hai Phong', 'DEVICE_D1', true);



INSERT INTO transactions (
    account_id, amount, currency, tx_type, merchant,
    device_id, ip_address, location, status
)
VALUES
(1, 1500000, 'VND', 'PAYMENT', 'Shopee', 'DEVICE_A1', '113.23.44.12', 'Hanoi', 'SUCCESS'),
(1, 2500000, 'VND', 'TRANSFER', 'MB Bank', 'DEVICE_A1', '113.23.44.12', 'Hanoi', 'SUCCESS');


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
