SELECT strftime(created_at, '%Y-%m') AS month, 
       SUM(CAST(current_total_price AS DECIMAL)) AS total_revenue 
FROM "shopify_data_orders_fake"
GROUP BY month 
ORDER BY month;
