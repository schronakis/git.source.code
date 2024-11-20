USE BetssonDB;
-----------------------------------------------------
CREATE TABLE stg.Customers(
	customer_id NVARCHAR(50) NOT NULL,
	hash_key NVARCHAR(50) NOT NULL
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE stg.Countries(
	sk_id INT NOT NULL,
	src_country_name NVARCHAR(100) NOT NULL,
	country_name NVARCHAR(100) NOT NULL,
	country_code NVARCHAR(3) NOT NULL,
	region NVARCHAR(100) NOT NULL,
	sub_region NVARCHAR(100) NOT NULL,
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE stg.Products(
	StockCode NVARCHAR(100)  NOT NULL,
	Description NVARCHAR(500) NULL,
	hash_key NVARCHAR(32) NULL
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE stg.Invoice(
	invoice INT NULL,
	quantity INT NULL,
	price DECIMAL(19, 2) NULL,
	country_sk_id INT NULL,
	product_sk_id INT NULL,
	customer_sk_id INT NULL,
	invoice_date DATE NULL,
	hash_key NVARCHAR(32) NULL
) ON [PRIMARY];
-----------------------------------------------------