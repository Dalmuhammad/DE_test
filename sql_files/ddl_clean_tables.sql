-- TABLE CUSTOMERS_CLEAN
DROP TABLE IF EXISTS customers_clean;

CREATE TABLE customers_clean (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    dob DATE,
    created_at DATETIME
);



-- TABLE SALES_CLEAN
DROP TABLE IF EXISTS sales_clean;

CREATE TABLE sales_clean (
    vin VARCHAR(50),
    customer_id INT,
    model VARCHAR(255),
    invoice_date DATE,
    price BIGINT,
    created_at DATETIME,
    
    INDEX idx_sales_vin (vin),
    INDEX idx_sales_customer (customer_id)
);



-- TABLE AFTER_SALES_CLEAN
DROP TABLE IF EXISTS after_sales_clean;

CREATE TABLE after_sales_clean (
    service_ticket VARCHAR(50) PRIMARY KEY,
    vin VARCHAR(50),
    customer_id INT,
    model VARCHAR(255),
    service_date DATE,
    service_type VARCHAR(255),
    created_at DATETIME,

    INDEX idx_service_vin (vin),
    INDEX idx_service_customer (customer_id)
);



-- TABLE CUSTOMER_ADDRESSES_CLEAN
DROP TABLE IF EXISTS customer_addresses_clean;

CREATE TABLE customer_addresses_clean (
    raw_id BIGINT PRIMARY KEY, 
    id INT,
    customer_id INT,
    address TEXT,
    city VARCHAR(100),
    province VARCHAR(100),
    ingestion_timestamp DATETIME,

    INDEX idx_address_customer (customer_id)
);




