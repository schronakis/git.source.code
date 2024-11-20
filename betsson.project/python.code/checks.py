from params import invoice_src_schema, path, filename
from utils import csv_to_pd
from checks_utils import check_datatypes, check_datatypes_values, check_for_duplicates

### Load Data
invoice_src_df = csv_to_pd(path, filename, encoding='ISO-8859-1', dtype=invoice_src_schema, parse_dates=['InvoiceDate']) 

### Check Columns DataTypes 
check_datatypes(invoice_src_df, 'Description')

### Check Values for a DataType
check_datatypes_values(invoice_src_df, 'Description', float)

### Check for Duplicates
check_for_duplicates(invoice_src_df, ['Invoice', 'Customer ID'], False)

### Various checks using FilterRows
check_row = invoice_src_df[(invoice_src_df['Invoice'] == '489514') & (invoice_src_df['StockCode'] == '20892')]
# print(check_row)
