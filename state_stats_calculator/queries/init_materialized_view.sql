CREATE MATERIALIZED VIEW kafka_to_transactions TO transactions_data AS
SELECT
    toFixedString(us_state, 2) AS us_state,
    cat_id,
    toDecimal64(amount, 2) AS total_amount
FROM kafka_queue
;
