SELECT sale_date, SUM(total_amount) AS daily_sales
FROM sales
GROUP BY sale_date;

SELECT c.city, SUM(s.total_amount) AS total_revenue
FROM sales s
JOIN customers c
ON s.customer_id = c.customer_id
GROUP BY c.city;

SELECT customer_id, COUNT(*) AS order_count
FROM sales
GROUP BY customer_id
HAVING COUNT(*) > 2;

SELECT cs.city, cs.customer_id, cs.first_name, cs.total_spend
FROM (
    SELECT c.city, s.customer_id, c.first_name, SUM(s.total_amount) AS total_spend
    FROM sales s
    JOIN customers c
    ON s.customer_id = c.customer_id
    GROUP BY c.city, s.customer_id, c.first_name
) cs
JOIN (
    SELECT city, MAX(total_spend) AS max_spend
    FROM (
        SELECT c.city, s.customer_id, SUM(s.total_amount) AS total_spend
        FROM sales s
        JOIN customers c
        ON s.customer_id = c.customer_id
        GROUP BY c.city, s.customer_id
    ) t
    GROUP BY city
) ms
ON cs.city = ms.city AND cs.total_spend = ms.max_spend;

SELECT c.customer_id, c.first_name, c.city,
       SUM(s.total_amount) AS total_spend,
       COUNT(s.sale_id) AS order_count
FROM sales s
JOIN customers c
ON s.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.city;
