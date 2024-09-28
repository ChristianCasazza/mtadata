SELECT strftime(created_at, '%Y-%m') AS month, 
       AVG(CAST(current_total_price AS DECIMAL)) AS average_order_value 
FROM "shopify_data_orders_fake"
GROUP BY month 
ORDER BY month;
