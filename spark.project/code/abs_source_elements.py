from abc import ABC, abstractmethod
from parameters import connection_info_files, connection_info_tables

class SourceElements(ABC):
    
    def apply(self, flag, schema):
        if flag == 'files':
            return self.get_flat_files(connection_info_files.get('csv_format'),
                                       schema,
                                       connection_info_files.get('source_files_path'),
                                       'Data8278.csv')
        elif flag == 'tables':
            return self.get_db_tables('stg.DimenLookupDwellStatus8278',
                                      schema,
                                      connection_info_tables.get('mssql_server'),
                                      connection_info_tables.get('mssql_port'),
                                      connection_info_tables.get('mssql_db'),
                                      connection_info_tables.get('mssql_user'),
                                      connection_info_tables.get('mssql_pswd'))

    @abstractmethod
    def get_flat_files(self):
        pass
    
    @abstractmethod
    def get_db_tables(self):
        pass