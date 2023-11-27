import pyspark.sql.functions as F
from source_schemas import dwell_data_schema, dwell_dim_schema
from get_sources import GetSourceElements

get_source_elements = GetSourceElements()      

source_files_df = get_source_elements.get_source_files('csv',dwell_data_schema,'/home/schronakis/git.source.code/spark.project/source_files/','Data8278.csv')
source_table_df = get_source_elements.get_db_tables('stg.DimenLookupDwellStatus8278',dwell_dim_schema,'192.168.1.10','1433','MyWork','schronakis','#3Chr0nakis')

print(source_files_df.columns)
print(source_table_df.columns)

rows = source_files_df.count()
print(f"DataFrame distinct row count: {rows}")

source_files_df.show()
source_table_df.show()

