SELECT billing_address__province AS province,
       COUNT(order_number) AS total_orders,
       SUM(CAST(current_total_price AS DECIMAL)) AS total_revenue,
       AVG(CAST(current_total_price AS DECIMAL)) AS average_order_value
FROM "shopify_data_orders_fake"
GROUP BY billing_address__province
ORDER BY total_revenue DESC;