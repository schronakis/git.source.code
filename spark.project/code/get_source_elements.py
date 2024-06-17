from abs_source_elements import SourceElements

class GetSourceElements(SourceElements):

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def get_flat_files(self, filename, schema, format, path):
        source_files_df = self.spark_session.read.format(format) \
            .option("header", True) \
            .schema(schema) \
            .load(f'{path}{filename}') 

        return source_files_df

    def get_db_tables(self, table, schema, format, server, port, database, username, password):
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
    
    def create_db_tables(self, df, table, server, port, database, username, password, insert_mode):
        ###### method passed with the creation of sql_user in SQL Server ######
        create_tables_df = df.write.format("jdbc") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("url", f"jdbc:sqlserver://{server}:{port};databaseName={database};trustServerCertificate=true") \
            .option("dbtable", table) \
            .option("user", username) \
            .option("password", password) \
            .mode(insert_mode) \
            .save()
        
        return create_tables_df