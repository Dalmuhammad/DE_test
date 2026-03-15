import time
from sqlalchemy import create_engine, text
from db_config import db_user, db_password, db_host, db_name

print("Waiting for MySQL to settle down...")
time.sleep(30)

# Config
engine = create_engine(
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
)

queries = [

"""
TRUNCATE TABLE customers_clean
""",

"""
INSERT INTO customers_clean
SELECT
    id,
    name,
    DATE_FORMAT(
        CASE
            WHEN dob IS NULL OR dob = '' OR dob = '1900-01-01' OR dob = '0000-00-00' THEN NULL
            WHEN dob LIKE '__/__/____' THEN STR_TO_DATE(dob,'%d/%m/%Y')
            WHEN dob LIKE '____/__/__' THEN STR_TO_DATE(dob,'%Y/%m/%d')
            ELSE STR_TO_DATE(dob,'%Y-%m-%d')
        END
    , '%Y-%m-%d') AS dob,
    created_at
FROM customers_raw
""",

"""
TRUNCATE TABLE sales_clean
""",

"""
INSERT INTO sales_clean
SELECT
    vin,
    customer_id,
    model,
    invoice_date,
    CAST(REPLACE(price,'.','') AS UNSIGNED) AS price,
    created_at
FROM sales_raw
""",

"""
TRUNCATE TABLE after_sales_clean
""",

"""
INSERT INTO after_sales_clean
SELECT
    service_ticket,
    vin,
    customer_id,
    model,
    service_date,
    service_type,
    created_at
FROM after_sales_raw
""",

"""
INSERT INTO customer_addresses_clean
SELECT
    raw_id,
    id,
    customer_id,
    address,
    lower(city) AS city,
    lower(province) AS province,
    ingestion_timestamp
FROM customer_addresses_raw
where  
    raw_id > (select coalesce(max(raw_id),0) from customer_addresses_clean) 
"""

]

with engine.begin() as conn:
    for q in queries:
        conn.execute(text(q))

print("Cleaning pipeline completed successfully.")