import time

import pandas as pd


def clickhouse_setup_mv(ch_client, logger, timeout=60):
    tables_to_initialize = ["kafka_queue", "transactions_data", "kafka_to_transactions"]

    with open("queries/init_materialized_view.sql", "r") as f:
        init_sql_script = f.read()

    queries = [q.strip() for q in init_sql_script.split(';') if q.strip()]
    for i, query in enumerate(queries):
        table_name = tables_to_initialize[i]
        logger.info(f"Executing setup query for {table_name}")
        try:
            ch_client.command(query)
        except Exception as e:
            logger.error(f"Error initializing table {table_name}: {e}")
            raise
        logger.info(f"Table {table_name} was succsessfully set up")

    start_time = time.time()
    loaded_size = 0
    while time.time() - start_time < timeout:
        result = ch_client.query("SELECT COUNT() AS loaded FROM transactions_data")
        loaded_size = result.first_item["loaded"] or 0

        logger.info("Loading data..")
        if loaded_size > 0:
            break
        time.sleep(2)

    processing_time = time.time() - start_time

    return loaded_size, processing_time
    


def clickhouse_calculate_stats(ch_client, output_path, logger):
    with open("queries/calculate_stats.sql", "r") as f:
        stats_query = f.read()

    logger.info("Executing stats query")
    try:
        start_time = time.time()
        result = ch_client.query(stats_query)
        calculation_time = time.time() - start_time
    except Exception as e:
        logger.error(f"Failed to calculate stats: {e}")
        raise
    logger.info("Stats query finished")

    df = pd.DataFrame(
        result.result_rows,
        columns=result.column_names,
    )

    df.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df)} lines to {output_path}")
    
    return calculation_time
