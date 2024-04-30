import os
import glob
import pandas as pd
from datetime import datetime
from misc_code.db_connect import db_connect

db_conn = db_connect('SQL Server', '172.16.10.64', 'MyWork', 'yes')

db_conn_cursor = db_conn.initiate_connection()

db_conn_cursor.execute("TRUNCATE TABLE stg.DimenLookupDwellStatus8278")

folder_location = r"C:\\Users\\schronakis\\Downloads\\enel\\"

df_columns = ['doc_id','unit_name', 'Max_netCAP_Publ','Max_netCAP_Est','Comments'] 

for filename in glob.glob(os.path.join(folder_location, '20231026_ISP1ISPResults_01.xlsx')):
    df = pd.read_excel(filename, engine = 'openpyxl',skiprows=0, skipfooter=0, usecols = "A:E", sheet_name='20231026_Activated Energy')
    df = df.fillna(' ')
print(df)

# df.columns = df_columns
# df['insert_timestamp'] = datetime.today().strftime('%Y-%m-%d %I:%M:%S')

# for index, row in df.iterrows():
#     db_conn_cursor.execute("INSERT INTO stg.ISP1UnitAvailabilities (doc_id, unit_name, Max_netCAP_Publ,Max_netCAP_Est,Comments,insert_timestamp) values(?,?,?,?,?,?)",row.doc_id, row.unit_name ,row.Max_netCAP_Publ, row.Max_netCAP_Est,row.Comments,row.insert_timestamp)

# db_conn.db_commit()
# db_conn.end_connection()
