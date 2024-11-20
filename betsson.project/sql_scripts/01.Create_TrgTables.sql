USE BetssonDB;
-----------------------------------------------------
CREATE TABLE trg.DimCountries (
    sk_id INT IDENTITY(1,1) PRIMARY KEY, 
    country_name NVARCHAR(100) NOT NULL,  
    country_code CHAR(3) NOT NULL,         
    region NVARCHAR(100) NULL,              
    sub_region NVARCHAR(100) NULL,
	start_date date NOT NULL,
	end_date date NOT NULL,
	is_active int
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE trg.DimCustomers(
    sk_id INT IDENTITY(1,1) PRIMARY KEY,
	customer_id NVARCHAR(50) NOT NULL,
	hash_key NVARCHAR(200) NOT NULL,
	start_date date NOT NULL,
	end_date date NOT NULL,
	is_active int
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE trg.DimProducts(
    sk_id INT IDENTITY(1,1) PRIMARY KEY, 
	stock_code NVARCHAR(100) NOT NULL,
	Description NVARCHAR(500) NULL,
	hash_key NVARCHAR(200) NOT NULL,
	start_date date NOT NULL,
	end_date date NOT NULL,
	is_active int
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE trg.FactInvoice(
	sk_id int IDENTITY(1,1) PRIMARY KEY,
	invoice INT NULL,
	quantity INT NULL,
	price DECIMAL(19, 2) NULL,
	country_sk_id INT NULL,
	product_sk_id INT NULL,
	customer_sk_id INT NULL,
	invoice_date DATE NULL,
	hash_key NVARCHAR(32) NULL,
	start_date date NOT NULL,
	end_date date NOT NULL
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE trg.DimTime(
	sk_id int IDENTITY(1,1) PRIMARY KEY,
    date_dt DATE NOT NULL,
    week_number INT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
) ON [PRIMARY];
-----------------------------------------------------
CREATE TABLE trg.ErrorCodes(
	sk_id int IDENTITY(1,1) PRIMARY KEY,
	error_value VARCHAR(100) NOT NULL,
	error_desc VARCHAR(1000) NOT NULL
) ON [PRIMARY];