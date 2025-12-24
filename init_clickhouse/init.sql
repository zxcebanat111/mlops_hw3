CREATE TABLE kafka_queue (
    cat_id String,
    amount Float64,
    us_state String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'clickhouse',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = '1',
    input_format_skip_unknown_fields = 1
;

CREATE TABLE transactions_data (
    us_state FixedString(2),
    cat_id LowCardinality(String),
    total_amount Decimal(15, 2)
) ENGINE = SummingMergeTree(total_amount)
ORDER BY (us_state, cat_id)
;
