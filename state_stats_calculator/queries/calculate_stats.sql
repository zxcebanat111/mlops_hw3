SELECT 
    us_state,
    argMaxMerge(top_category) AS top_category,
    max(max_amount) AS max_transaction
FROM transactions_data
GROUP BY us_state
ORDER BY max_transaction DESC
