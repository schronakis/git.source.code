USE BetssonDB;
---------------------------------------------
TRUNCATE TABLE stg.Customers;
---------------------------------------------
WITH A AS
(SELECT ISNUMERIC(CUSTOMERID) AS CUS_INT,
        CUSTOMERID
 FROM src.Customers P)

INSERT INTO stg.Customers
SELECT CUSTOMERID, 
	   CONVERT(NVARCHAR(32),HashBytes('MD5', CUSTOMERID),2) AS HASH_KEY
FROM A
WHERE CUS_INT = 1;
