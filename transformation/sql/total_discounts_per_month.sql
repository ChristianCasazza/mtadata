SELECT strftime(created_at, '%Y-%m') AS month, 
       SUM(CAST(total_discounts AS DECIMAL)) AS total_discounts 
FROM "shopify_data_orders_fake"
GROUP BY month 
ORDER BY month;
