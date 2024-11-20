########################### Define SourcePaths ###########################
path = 'betsson.project/src_files/'
filename = 'Invoices_Year_2009-2010.csv'

########################### Define Source Schemas ###########################
invoice_src_schema = {
    'Invoice': str,
    'StockCode': str,  
    'Description': str, 
    'Quantity': int,     
    'Price': float,      
    'Customer ID': str,    
    'Country': str    
}

########################### Create Statements ###########################

product_create = """
        CREATE TABLE src.Products (
            StockCode NVARCHAR(100),
            Description NVARCHAR(500)
        )"""

customer_create = """
        CREATE TABLE src.Customers (
            CustomerID NVARCHAR(100)
        )"""

invoice_create = """
        CREATE TABLE src.Invoice (
            Invoice Integer,
            Quantity Integer,
            Price Decimal(19,2),
            StockCode NVARCHAR(100),
            CustomerID NVARCHAR(100),
            Country NVARCHAR(100),
            InvoiceDate DATETIME
        )"""
