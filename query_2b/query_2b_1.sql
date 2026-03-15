-- Query 2b table 1
SELECT
    DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
    CASE
        WHEN price >= 100000000 AND price < 250000000 THEN 'LOW'
        WHEN price >= 250000000 AND price <= 400000000 THEN 'MEDIUM'
        WHEN price > 400000000 THEN 'HIGH'
    END AS class,
    model,
    SUM(price) AS total
FROM sales_clean
GROUP BY
    DATE_FORMAT(invoice_date, '%Y-%m'),
    class,
    model
ORDER BY
    periode,
    class,
    model;