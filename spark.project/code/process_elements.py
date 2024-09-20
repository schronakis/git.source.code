from get_source_elements import GetSourceElements
from pyspark.sql import functions as F
from spark_session import sparkSession
from utils import convertCase, upperCase
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType
from source_schemas import init_client_schema, complete_client_schema
from parameters import connection_info_tables

class ProcessElements:
        def __init__(self):
             sc = sparkSession('R&D Spark','2g','2g','4')
             self.spark_session = sc.generate_spark_session()
             self.get_elements = GetSourceElements(self.spark_session)
        
        def get_init_clients_df(self):
              init_clients_df = self.get_elements.apply('table', 'dbo.client', init_client_schema)
              init_clients_df = init_clients_df \
                .withColumn('client_id', F.concat(F.lit('C'), F.lpad(F.col('client_id'),8,'0')))
              return init_clients_df
 
        def get_complete_clients_df(self):  
              complete_clients_df = self.get_elements.apply('file', 'completedclient.csv', complete_client_schema, 'csv')
              
              return complete_clients_df             

        def get_enriched_clients_df(self):
              init_client_df = self.get_init_clients_df()
              complete_client_df = self.get_complete_clients_df()

              upper_all_chars = F.udf(upperCase, StringType())
              upper_init_char = F.udf(convertCase, StringType())

              enriched_clients_df = init_client_df.alias('init_clients') \
                .join(complete_client_df.alias('complete_clients'), F.col('init_clients.client_id') == F.col('complete_clients.client_id'), 'inner') \
                .withColumn('fullname', F.concat_ws(' ', F.col('complete_clients.last'),F.col('complete_clients.first'))) \
                .select(F.col('complete_clients.client_id'),
                        upper_all_chars(F.col('fullname')).alias('fullname'),
                        F.col('complete_clients.fulldate').alias('birthday_date'),
                        F.col('complete_clients.social').alias('social_number'),
                        F.col('complete_clients.phone'),
                        F.col('complete_clients.email'),
                        F.col('complete_clients.address_1').alias('address'),
                        F.col('complete_clients.city'),
                        F.col('complete_clients.zipcode'),
                        F.col('complete_clients.district_id'))

              return enriched_clients_df
        
        def save_df_to_db(self, table_name, mode):
              self.get_elements.create_db_tables(self.get_enriched_clients_df(),
              table_name,
              connection_info_tables.get('mssql_server'),
              connection_info_tables.get('mssql_port'),
              connection_info_tables.get('mssql_db'),
              connection_info_tables.get('mssql_user'),
              connection_info_tables.get('mssql_pswd'),
              mode)

        #      .filter((F.col('DwellRecType').isin(1,2)) & 
        #              (F.col('DwellStatus').isin(2,11)) &
        #              (F.col('Area').isin(77,1)) &
        #              (F.col('Count').isin(159222,405))) \
        #      .distinct() 

        #      data_df = self.get_sources_to_df('file', 'Data8278.csv', dwell_data_schema, 'csv')
        #      rectype_df = self.get_sources_to_df('file', 'DimenLookupDwellRecType8278.csv', dwell_dim_schema, 'csv')
        #      status_df = self.get_sources_to_df('table', 'stg.DimenLookupDwellStatus8278', dwell_dim_schema)             
        #      concat_df = data_df.alias('data_df') \
        #      .join(rectype_df.alias('rectype_df'), F.col('DwellRecType') == F.col('rectype_df.Code'), 'inner') \
        #      .join(status_df.alias('status_df'), F.col('DwellStatus') == F.col('status_df.Code'), 'inner') \
        #      .select(('data_df.*'),
        #              upper_init_char(F.col('rectype_df.Description')).alias('RecTypeDescription'),
        #              upper_all_chars(F.col('status_df.Description')).alias('StatusDescription')) \
        #      .filter((F.col('DwellRecType') == 1) & 
        #              (F.col('DwellStatus') == 2) &
        #              (F.col('Area') == 77) &
        #              (F.col('Count') == 159222))

t = ProcessElements()
df = t.save_df_to_db('enriched_clients', 'overwrite')
# df.show()