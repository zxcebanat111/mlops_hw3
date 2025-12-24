import pandas as pd


def load_data_to_kafka(producer, topic_name, path_to_data, logger, batch_size=None, verbose_freq=10000):
    num_lines = 0
    try:
        logger.info("Started loading data")
        for chunk in pd.read_csv(path_to_data, chunksize=batch_size):
            records = chunk.to_dict('records')

            for record in records:
                producer.send(topic_name, value=record)
                num_lines += 1
                if num_lines % verbose_freq == 0:
                    logger.info(f"Loaded {num_lines} lines")
    except Exception as e:
        logger.error(f"Error sending data: {e}")
        raise

    return num_lines
