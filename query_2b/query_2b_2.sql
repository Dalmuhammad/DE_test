-- query 2b table 2
WITH latest_address AS (
    SELECT
        customer_id,
        address,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY ingestion_timestamp DESC
        ) AS rn
    FROM customer_addresses_clean
)
SELECT
    DATE_FORMAT(a.service_date,'%Y') AS periode,
    a.vin,
    c.name AS customer_name,
    la.address,
    COUNT(a.service_ticket) AS count_service,
    CASE
        WHEN COUNT(a.service_ticket) > 10 THEN 'HIGH'
        WHEN COUNT(a.service_ticket) BETWEEN 5 AND 10 THEN 'MED'
        ELSE 'LOW'
    END AS priority
FROM after_sales_clean a
JOIN sales_clean s
    ON a.vin = s.vin
JOIN customers_clean c
    ON s.customer_id = c.id
LEFT JOIN latest_address la
    ON c.id = la.customer_id
    AND la.rn = 1
GROUP BY
    DATE_FORMAT(a.service_date,'%Y'),
    a.vin,
    c.name,
    la.address
ORDER BY
    periode,
    count_service DESC;