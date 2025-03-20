from Create_spark import create_spark
import logs.levels as log

spark = create_spark()
def create_dataframe(path):
    try:
        table_name = spark.read.csv(path, header=True, inferSchema=True)
        log.log_info(f'{table_name} created Successfully...')
        return table_name
    except Exception as e:
        log.log_error(f'Error for while creating dataframe : {table_name}, error: {e}')
