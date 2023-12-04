from abc import ABC, abstractmethod
from parameters import connection_info_files, connection_info_tables

class SourceElements(ABC):
    
    def apply(self, flag, element_name, schema, format=None):
        if flag == 'file':
            return self.get_flat_files(element_name,
                                       schema,
                                       format,
                                       connection_info_files.get('source_files_path'))
        elif flag == 'table':
            return self.get_db_tables(element_name,
                                      schema,
                                      format,
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