import json
import logging
import os

import clickhouse_connect
from kafka import KafkaProducer

from lib.clickhouse import (
    clickhouse_calculate_stats,
    clickhouse_setup_mv,
)
from lib.load_data import load_data_to_kafka


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_NAME = os.getenv("TOPIC_NAME")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME", "click")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "click")

PATH_TO_DATA = os.getenv("PATH_TO_DATA", "./data/input.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "./output/transactions_stats_by_states.csv")

BATCH_SIZE = os.getenv("BATCH_SIZE")


class StateStatsCalculator:
    def __init__(self, logger):
        self.logger = logger

        self.ch_data_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        self.ch_topic_name = TOPIC_NAME

        self.ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USERNAME,
            password=CLICKHOUSE_PASSWORD,     
        )


    def calculate_stats(self, path_to_data, output_path):
        size = load_data_to_kafka(
            producer=self.ch_data_producer,
            topic_name=self.ch_topic_name,
            path_to_data=path_to_data,
            logger=self.logger,
            batch_size=int(BATCH_SIZE) if BATCH_SIZE else None,
        )
        self.logger.info(f"Loaded transactions data with {size} rows")

        loaded_size, processing_time = clickhouse_setup_mv(
            ch_client=self.ch_client,
            logger=self.logger,
        )

        logger.info(f"Aggregated transactions data by states and cat_id in {processing_time:2f}s. Got {loaded_size} rows")
        
        calculation_time = clickhouse_calculate_stats(ch_client=self.ch_client, output_path=output_path, logger=self.logger)

        self.logger.info(f"Calculated stats in {calculation_time:2f}s")
        

if __name__ == "__main__":
    logger.info("Starting...")
    service = StateStatsCalculator(logger)

    try:
        service.calculate_stats(path_to_data=PATH_TO_DATA, output_path=OUTPUT_PATH)
    except KeyboardInterrupt:
        logger.info("Service interrupted")
        
