###### importing required modules ######
from pypdf import PdfReader
from pathlib import Path
import re
import pyodbc

####### db_connection #######
driver = 'SQL Server'
server = 'E352012'
database = 'MyWork'
trusted = 'yes'

conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};Trusted_Connection={trusted};')
cursor = conn.cursor()
#############################

pdf_path = (
     Path.home()
     / "C:\\Users\\schronakis\\Downloads\\enel\\"
     / "20230101_20230131_15minAverageFRCE_01.pdf")

pdf_reader = PdfReader(pdf_path)

###### read single page ######
# page = pdf_reader.pages[0]
# page_content = page.extract_text()
# data = json.dumps(page_content)
# data_json = json.loads(data)

###### read all pages ######
page_content = ''
number_of_pages = len(pdf_reader.pages)
for page_number in range(number_of_pages):
    page = pdf_reader.pages[page_number]
    page_content += page.extract_text() 

page_content_re = re.sub('15 min average FRCE - Ιανουάριος 2023', '\n', page_content).replace('Time CET', 'TimeCET').replace(' ',', ')
page_content_to_list = (page_content_re.splitlines())

columns = page_content_to_list[0].replace('/','_').replace(' ','')
columns_split = columns.split(',')
columns_prefix = [f'a_{i}' for i in columns_split]
columns_joined = ', '.join(columns_prefix)
print(columns_joined)
columns_tuple = (tuple(map(str, columns_joined.split(', '))))

for i in page_content_to_list[1:]:
    i_tuple = tuple(map(str, i.split(', ')))
    # conn.execute('INSERT INTO stg.enel_FRCE_01(' + columns_joined + ') VALUES (' + ','.join('?' * len(i_tuple)) + ')' ,i_tuple)

conn.commit()
conn.close()

# print(data)
# print(data_json)
# print(len(pdf_reader.pages))
# print(pdf_reader.metadata)

# print(len(columns_tuple) )
# print(','.join('?' * len(columns_tuple)))
