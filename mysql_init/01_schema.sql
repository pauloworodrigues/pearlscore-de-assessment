-- NordStack billing system (simulated source for step 0).
-- Runs automatically on MySQL's FIRST boot only (mounted at
-- /docker-entrypoint-initdb.d in docker-compose.yml).
--
-- Deliberately loosely typed. See docs/DATA_QUALITY.md for the 13 planted
-- defects this schema is designed to let through untouched.

CREATE TABLE IF NOT EXISTS customers (
    _row_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id   VARCHAR(64),
    customer_name VARCHAR(255),
    email         VARCHAR(255),
    country       VARCHAR(64),
    created_at    VARCHAR(64),
    updated_at    DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    _row_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    subscription_id VARCHAR(64),
    customer_id     VARCHAR(64),
    plan_name       VARCHAR(64),
    monthly_price   VARCHAR(64),
    start_date      VARCHAR(64),
    end_date        VARCHAR(64),
    status          VARCHAR(64),
    updated_at      DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS invoices (
    _row_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    invoice_id      VARCHAR(64),
    subscription_id VARCHAR(64),
    invoice_date    VARCHAR(64),
    amount          VARCHAR(64),
    currency        VARCHAR(16),
    status          VARCHAR(64),
    updated_at      DATETIME NOT NULL
);

-- Explicit grant, rather than relying solely on the MySQL image's implicit
-- privileges for MYSQL_USER: makes the access billing_user needs
-- reproducible and visible in version control, instead of depending on
-- image-version behavior we don't control.
GRANT SELECT, INSERT, UPDATE, DELETE ON billing.* TO 'billing_user'@'%';
FLUSH PRIVILEGES;
