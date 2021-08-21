import sys
import os
import logging
LOGGER = logging.getLogger('jupyter_logger')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
CUR_PATH = os.path.dirname(__file__)
sys.path.append(os.path.join(CUR_PATH, '..', '..'))
from modules.some_module.config import CONFIG as config
from modules.some_module.module import Pipeline

if __name__ == '__main__':
    config.parse_arguments()
    config.tune_logger(LOGGER)

    spark = SparkSession.builder.master('local[1]').appName('my_app').getOrCreate()

    try:
        df = spark.createDataFrame([['555', 13.4]], ['zrtpluid', 'zprice'])
        argument_tables = {'input_table_1': df}  # Словарь с таблицами-аргументами

        pipeline = Pipeline(spark, config, logger=LOGGER, test_arguments=argument_tables)
        result = pipeline.run()
        result['out_table_2'].show()
    finally:
        spark.stop()
