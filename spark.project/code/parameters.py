import os

###################################### env_variables ############################################
# Set environment variables
os.environ['source_files_path'] = '/home/schronakis/git.source.code/spark.project/source_files/'

# Get environment variables
file_format = os.getenv('file_format')
#################################################################################################

###################################### connection_info ##########################################
connection_info_files = {
    'csv_format':'csv',
    'parquet_format':'parquet',
    'json_format':'json',
    'source_files_path':'/home/schronakis/git.source.code/spark.project/source_files/'
    }

connection_info_tables = {
    'mssql_server':'192.168.1.10',
    'mssql_port':'1433',
    'mssql_db':'MyWork',
    'mssql_user':'schronakis',
    'mssql_pswd':'#3Chr0nakis'
    }
#################################################################################################
