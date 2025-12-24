SELECT 
    us_state,
    argMax(cat_id, total_amount) AS top_category,
    MAX(total_amount) AS max_total
FROM transactions_data
GROUP BY us_state
ORDER BY max_total DESC
