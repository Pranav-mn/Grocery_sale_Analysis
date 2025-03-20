from pyspark.sql import SparkSession
import logs.levels as log

def create_spark():
    try:
        spark = SparkSession.builder \
            .appName('Grocery sales').getOrCreate()
        log.log_info('Spark Application Started...')
        return spark
    except Exception as e:
        log.log_error(f'Unable to create Spark Application, {e}')
