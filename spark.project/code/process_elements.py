from get_source_elements import GetSourceElements
from pyspark.sql import functions as F
from spark_session import sparkSession
from source_schemas import dwell_data_schema, dwell_dim_schema

class ProcessElements:
        def __init__(self):
                sc = sparkSession('R&D Spark','2g','2g','4')
                self.spark_session = sc.generate_spark_session()

        def get_sources_to_df(self, file_type, filename, schema, format=None):
             self.get_elements = GetSourceElements(self.spark_session)
             src_df = self.get_elements.apply(file_type, filename, schema, format)
             
             return src_df   

        def get_concat_data_df(self):
                data_df = self.get_sources_to_df('file', 'Data8278.csv', dwell_data_schema, 'csv')

                final_df = data_df.alias('data_df') \
                .withColumn('Year_Count', F.concat_ws('-', F.col('Year'),F.col('Count'))) \
                .filter((F.col('DwellRecType').isin(1,2)) & 
                        (F.col('DwellStatus').isin(2,11)) &
                        (F.col('Area').isin(77,1)) &
                        (F.col('Count').isin (159222,405))) \
                .distinct() 

                return final_df

        def get_joined_df(self):
                data_df = self.get_sources_to_df('file', 'Data8278.csv', dwell_data_schema, 'csv')
                rectype_df = self.get_sources_to_df('file', 'DimenLookupDwellRecType8278.csv', dwell_dim_schema, 'csv')
                status_df = self.get_sources_to_df('table', 'stg.DimenLookupDwellStatus8278', dwell_dim_schema) 

                concat_df = data_df.alias('data_df') \
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

                return concat_df


# process_elements = ProcessElements()
# process_elements.get_sources_to_df('file', 'Data8278.csv', dwell_data_schema, 'csv').show()