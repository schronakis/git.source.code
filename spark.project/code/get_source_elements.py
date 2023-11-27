from abs_source_elements import SourceElements
from spark_session import sparkSession
from source_schemas import dwell_data_schema, dwell_dim_schema

class GetSourceElements(SourceElements):
    sc = sparkSession('RnD Spark','2g','2g','4')
    spark_session = sc.generate_spark_session()

    def get_flat_files(self, format, schema, path, filename):
        source_files_df = self.spark_session.read.format(format) \
        .option("header", True) \
        .schema(schema) \
        .load(f'{path}{filename}') 

        return source_files_df

    def get_db_tables(self, table, schema, server, port, database, username, password):
        ###### method passed with the creation of sql_user in SQL Server ######
        source_tables_df = self.spark_session.read.format("jdbc") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", f"jdbc:sqlserver://{server}:{port};databaseName={database};trustServerCertificate=true") \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .option("schema", schema) \
            .load()
        
        return source_tables_df

get_elements = GetSourceElements()
# df = get_elements.apply('files',dwell_data_schema)
df = get_elements.apply('tables',dwell_dim_schema)
df.show()