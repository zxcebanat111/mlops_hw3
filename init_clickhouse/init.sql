CREATE TABLE kafka_queue (
    cat_id LowCardinality(String),
    amount Decimal(7, 2),
    us_state FixedString(2)
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
    max_amount SimpleAggregateFunction(max, Decimal(7, 2)),
    top_category AggregateFunction(argMax, LowCardinality(String), Decimal(7, 2))
) ENGINE = AggregatingMergeTree()
ORDER BY us_state;
;
