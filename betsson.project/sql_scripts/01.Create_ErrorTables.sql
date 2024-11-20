USE [BetssonDB];
------------------------------------------
CREATE TABLE [dbo].[ProductsNoDesc](
	[STOCKCODE] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](500) NULL
) ON [PRIMARY];
------------------------------------------
CREATE TABLE [dbo].[ProductsDuplicates](
	[STOCKCODE] [nvarchar](100) NULL,
	[DESCRIPTION] [nvarchar](500) NULL
) ON [PRIMARY];
------------------------------------------
CREATE TABLE [dbo].[InvoiceQualityNegative](
	[StockCode] [nvarchar](100) NULL,
	[Quantity] [int] NULL,
	[Price] [decimal](19, 2) NULL
) ON [PRIMARY];
------------------------------------------
CREATE TABLE [dbo].[CustomersInvalid](
	[CUSTOMERID] [nvarchar](100) NULL
) ON [PRIMARY];
------------------------------------------
CREATE TABLE [dbo].[CountriesInvalid](
	[country_name] [nvarchar](100) NULL
) ON [PRIMARY];
------------------------------------------
CREATE TABLE [dbo].[InvoicePriceNegative](
	[StockCode] [nvarchar](100) NULL,
	[Quantity] [int] NULL,
	[Price] [decimal](19, 2) NULL
) ON [PRIMARY];