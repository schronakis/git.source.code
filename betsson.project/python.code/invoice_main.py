import pandas as pd
import sys
from db_connect import db_connect
from params import invoice_src_schema, path, filename, product_create, customer_create, invoice_create
from utils import csv_to_pd, dataframe_to_database

################################### Acquire Source Data ########################################
print(f'Acquire SourceFile: {filename} -- Start')
invoice_src_df = csv_to_pd(path, filename, encoding='ISO-8859-1', dtype=invoice_src_schema, parse_dates=['InvoiceDate'])
print(f'Acquire SourceData: {filename} -- Finish :: Count:{len(invoice_src_df)}')
################################################################################################

################################### Columns Transformations ####################################
print('Column Transformations -- Start')
try:
    print('From Invoice Column any values beginning with character was cleared and converted to Integer')
    invoice_src_df['Invoice'] = invoice_src_df['Invoice'].apply(lambda x: x[1:] if x[0].isalpha() else x) 
    invoice_src_df['Invoice'] = invoice_src_df['Invoice'].astype(int)

    print('Convert StockCode Column to String')
    invoice_src_df['StockCode'] = invoice_src_df['StockCode'].astype('string')

    print('Convert Description Column from Float to String')
    invoice_src_df['Description'] = invoice_src_df['Description'].apply(lambda x: str(x) if isinstance(x, float) and pd.notna(x) else x)
    invoice_src_df['Description'] = invoice_src_df['Description'].fillna('')  
    invoice_src_df['Description'] = invoice_src_df['Description'].astype('string')

    print('Convert Customer ID Column from Float to String and Rename it to CustomerID')
    invoice_src_df['Customer ID'] = invoice_src_df['Customer ID'].apply(lambda x: str(x) if isinstance(x, float) and pd.notna(x) else x)
    invoice_src_df['Customer ID'] = invoice_src_df['Customer ID'].fillna('') 
    invoice_src_df['Customer ID'] = invoice_src_df['Customer ID'].astype('string')
    invoice_src_df = invoice_src_df.rename(columns={'Customer ID':'CustomerID'})

    print('Convert Country to String')
    invoice_src_df['Country'] = invoice_src_df['Country'].fillna('') 
    invoice_src_df['Country'] = invoice_src_df['Country'].astype('string')

    print('Convert InvoiceDate to DateTime')
    invoice_src_df['InvoiceDate'] = pd.to_datetime(invoice_src_df['InvoiceDate'])
    print('Column Transformations -- Finish')

except Exception as e:
    print(f'An Error in Column Transformations Occurred: {e}')
    sys.exit(1)
################################################################################################

################################################################################################
print('Create DataFrames to load them to source tables -- Start')
try:
    print('Create Product dataframe -- Start')
    product = invoice_src_df[['StockCode', 'Description']].drop_duplicates()
    print(f'Create Product dataframe -- Finish :: Count: {len(product)}')

    print('Create Customer dataframe -- Start')
    customer = invoice_src_df[['CustomerID']].drop_duplicates()
    print(f'Create Customer dataframe -- Finish :: Count: {len(customer)}')

    print('Create Invoice dataframe -- Start')
    invoice = invoice_src_df[['Invoice', 'Quantity', 'Price', 'StockCode', 'CustomerID', 'Country', 'InvoiceDate']].drop_duplicates()
    print(f'Create Invoice dataframe -- Finish :: Count: {len(invoice)}')
    print('Create DataFrames to load them to source tables -- Finish')

except Exception as e:
    print(f'An Error Creating the Dataframes Occurred: {e}')
    sys.exit(1)
################################################################################################

################################################################################################
print('Initiate DataBase Connection')

db_conn = db_connect( 'SQL Server', 'PW0B6TR5\MSSQL_2022', 'BetssonDB', 'yes')
init_conn = db_conn.initiate_connection()

dataframe_to_database(db_conn, init_conn, product_create, 'src', 'Products', product)
dataframe_to_database(db_conn, init_conn, customer_create, 'src', 'Customers', customer)
dataframe_to_database(db_conn, init_conn, invoice_create, 'src', 'Invoice', invoice)

print('Jobs on Database were Completed Successfully')