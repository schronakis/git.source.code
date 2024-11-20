import os

###################################### env_variables ############################################
# Set environment variables
os.environ['source_files_path'] = '/home/schronak/git.source.code/spark.project/source_files/bank_refined/'

# Get environment variables
files_path = os.getenv('source_files_path')
database_url = os.environ.get('mssql_server_ip', '10.249.18.152') # 192.168.1.8:HOME_IP
#################################################################################################

###################################### connection_info ##########################################
connection_info_files = {
    'csv_format':'csv',
    'parquet_format':'parquet',
    'json_format':'json',
    'source_files_path':'/home/schronak/git.source.code/spark.project/source_files/bank_refined/'
    }

connection_info_tables = {
    'mssql_server': database_url, # (work) or (home)
    'mssql_port':'1433',
    'mssql_db':'TrainingDB',
    'mssql_user':'schronakis',
    'mssql_pswd':'@2Stavr0s'
    }
#################################################################################################