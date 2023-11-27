from spark_session import sparkSession

class GetSourceElements:

    def __init__(self):
        sc = sparkSession('RnD Spark','2g','2g','4')
        self.spark_session = sc.generate_spark_session()
    
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
    
    def get_source_files(self, format, schema, path, filename):
        source_files_df = self.spark_session.read.format(format) \
        .option("header", True) \
        .schema(schema) \
        .load(f'{path}{filename}') 

        return source_files_df
