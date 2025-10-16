-- check rows count for fact_orders table
SELECT COUNT(*) FROM fact_orders AS fact_orders_count
-- check rows count for fact_orders_items table
SELECT COUNT(*) FROM fact_orders_items AS fact_orders_items_count
-- check rows count for fact_payments table
SELECT COUNT(*) FROM fact_payments AS fact_payments_count
-- check rows count for fact_reviews table
SELECT COUNT(*) FROM fact_reviews AS fact_reviews_count
-- check rows count for dim_customers table
SELECT COUNT(*) FROM dim_customers AS dim_customers_count
-- check rows count for dim_date table
SELECT COUNT(*) FROM dim_date AS dim_date_count
-- check rows count for dim_payments table
SELECT COUNT(*) FROM dim_payments AS dim_payments_count
-- check rows count for dim_products table
SELECT COUNT(*) FROM dim_products AS dim_seller
-- check rows count for dim_seller table
SELECT COUNT(*) FROM dim_seller AS dim_seller

--check orders with missing customers in fact table
SELECT COUNT(*)
FROM fact_orders f1 
LEFT JOIN dim_customers c1
ON f1.customer_sk=c1.customer_sk
WHERE c1.customer_sk IS NULL

-- CHECK ORDER ITEMS WITH MISSING PRODUCTS
SELECT COUNT(*)
FROM dim_products p1
LEFT JOIN fact_orders_items f1 
ON p1.product_sk=f1.product_sk
WHERE f1.product_sk IS NULL 

--check if there are repeated surrogate keys in customer table
SELECT COUNT(*)
FROM dim_customers
GROUP BY customer_sk
HAVING COUNT(*) >1

-- SHOW REVENUE BY PRODUCT CATEGORY AND YEAR , MONTH
SELECT  dp1.product_category_name,
SUM (fai1.price +fai1.freight_value) AS total_price,
dd.year,dd.month AS month
date
FROM fact_orders_items fai1
JOIN fact_orders fa ON fa.order_id=fai1.order_id
JOIN dim_products dp1 ON fai1.product_sk=dp1.product_sk
JOIN dim_date dd ON dd.date_id=fa.purchase_date_id
GROUP BY dd.year,dd.month,dp1.product_category_name
ORDER BY dd.year,dd.month,dp1.product_category_name DESC

-- SHOW Top 10 sellers by revenue1

SELECT ds.seller_city,ds.seller_state,
SUM(fa.price+fa.freight_value) AS TOTAL_PRICE
FROM fact_orders_items fa
JOIN dim_seller ds ON fa.seller_sk=ds.seller_sk
GROUP BY ds.seller_city,ds.seller_state
ORDER BY ds.seller_city,ds.seller_state
LIMIT 10

-- SHOW AVERAGE DELIVERY DAYS 
SELECT AVG (delivery_days) AS average_delivery,
       AVG (delay_days) AS average_delays
FROM fact_orders
WHERE delay_days IS NOT NULL

--SHOW REVIEW DISTRIBUTION
SELECT review_score , COUNT(*) AS review_count
FROM fact_reviews
GROUP BY review_score
ORDER BY review_score

-- SHOW DISTRIBUTION OF PAYMENT METHOD
SELECT dp.payment_type,
COUNT(*) AS payment_count,
SUM(payment_value) AS total_value
FROM dim_payments dp
JOIN fact_payments fp ON dp.payment_sk=fp.payment_sk
GROUP BY dp.payment_type

--SHOW ORDERS WITH MULTIPLE PAYMENTS
SELECT order_id, 
COUNT (*) AS orders_multiple_payments
FROM fact_payments 
GROUP BY order_id
HAVING COUNT(*) >1

--SHOW CUSTOMERS THAT MAKE REPEAT PURCHASE
SELECT customer_sk, COUNT(DISTINCT order_id) AS num_orders
FROM fact_orders
GROUP BY customer_sk
HAVING COUNT(DISTINCT order_id) > 1
ORDER BY num_orders DESC;



