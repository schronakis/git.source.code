from get_source_elements import GetSourceElements
from pyspark.sql import functions as F
from spark_session import sparkSession
from source_schemas import dwell_data_schema, dwell_dim_schema

sc = sparkSession('R&D Spark','2g','2g','4')
spark_session = sc.generate_spark_session()

get_elements = GetSourceElements(spark_session)

data_df = get_elements.apply('file', 'Data8278.csv', dwell_data_schema)
rectype_df = get_elements.apply('file', 'DimenLookupDwellRecType8278.csv', dwell_dim_schema)
status_df = get_elements.apply('table', 'stg.DimenLookupDwellStatus8278', dwell_dim_schema)


data_df.alias('data_df') \
    .join(rectype_df.alias('rectype_df'), F.col('DwellRecType') == F.col('rectype_df.Code'), 'inner') \
    .join(status_df.alias('status_df'), F.col('DwellStatus') == F.col('status_df.Code'), 'inner') \
    .select(('data_df.*'),
            F.col('rectype_df.Description').alias('RecTypeDescription'),
            F.col('status_df.Description').alias('StatusDescription')) \
    .filter((F.col('DwellRecType') == 1) & 
            (F.col('DwellStatus') == 2) &
            (F.col('Area') == 77) &
            (F.col('Count') == 159222)) \
    .distinct() \
    .show()


data_df.alias('data_df') \
    .filter((F.col('DwellRecType') == 1) & 
            (F.col('DwellStatus') == 2) &
            (F.col('Area') == 77) &
            (F.col('Count') == 159222)) \
    .distinct() \
    .show()