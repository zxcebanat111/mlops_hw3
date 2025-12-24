CREATE MATERIALIZED VIEW kafka_to_transactions TO transactions_data AS
SELECT 
    us_state,
    max(amount) AS max_amount,
    argMaxState(cat_id, amount) AS top_category
FROM kafka_queue
GROUP BY us_state;
