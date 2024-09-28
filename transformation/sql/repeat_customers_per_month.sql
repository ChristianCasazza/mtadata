SELECT strftime(created_at, '%Y-%m') AS month, 
       COUNT(DISTINCT customer__id) AS repeat_customers
FROM "shopify_data_orders_fake"
GROUP BY month 
HAVING repeat_customers > 1
ORDER BY month;
