-- 1. Total amount for each customer
SELECT customer_id, SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id;
-- 2. Top 3 customers by total spend
SELECT customer_id, SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id
ORDER BY total_amount DESC
LIMIT 3;
-- 3. Customers with no orders
SELECT c.customer_id, c.customer_name, c.city, c.age
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;
-- 4. City-wise total revenue
SELECT c.city, SUM(o.amount) AS total_revenue
FROM customers c
INNER JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.city;
-- 5. Average order amount per customer
SELECT c.customer_id, c.customer_name, AVG(o.amount) AS avg_amount_per_customer
FROM customers c
INNER JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;
-- 6. Customers with more than one order
SELECT c.customer_id, c.customer_name, COUNT(*) AS orders
FROM customers c
INNER JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(*) > 1;
-- 7. Sort customers by total spend (descending)
SELECT customer_id, SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id
ORDER BY total_amount DESC;
