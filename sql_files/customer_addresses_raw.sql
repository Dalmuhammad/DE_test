-- DROP TABLE
DROP TABLE IF EXISTS customer_addresses_raw;

-- CREATE TABLE
CREATE TABLE customer_addresses_raw (
    raw_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    id INT,
    customer_id INT,
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    created_at DATETIME,
    source_file VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
