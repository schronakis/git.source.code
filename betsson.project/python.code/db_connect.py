import pyodbc

class db_connect:

    def __init__(self, driver, server, database, trusted):
        self.driver = driver
        self.server = server
        self.database = database
        self.trusted = trusted

        self.conn = pyodbc.connect(f'Driver={self.driver};Server={self.server};Database={self.database};Trusted_Connection={self.trusted};')
    

    def generate_db_schema(self, _schema):
        generate_schema_sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{_schema}')
        BEGIN 
            EXEC('CREATE SCHEMA {_schema}');
        END
        """
        return generate_schema_sql


    def generate_db_table(self, create_table_sql, _schema, _table):
        generate_table_sql = f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{_schema}' and TABLE_NAME = '{_table}')
        BEGIN 
            {create_table_sql} 
        END
        """
        return generate_table_sql
    
    
    def truncate_db_table(self, _table):
        truncate_table_sql = f"""TRUNCATE TABLE {_table}"""
        return truncate_table_sql
    
    
    def insert_data(self, _table, _df, _columns, _values):
        insert_query = f"""
        INSERT INTO {_table} {_columns}
        VALUES {_values}
        """

        tuple_columns = tuple(_columns.strip('()').replace(" ", "").split(','))
        batch_data = [
            tuple(row[col] for col in tuple_columns)
            for _, row in _df.iterrows()]

        return insert_query, batch_data
    
    
    def initiate_connection(self):
        return self.conn.cursor()
