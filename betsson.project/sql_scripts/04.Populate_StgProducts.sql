USE BetssonDB;
---------------------------------------------
TRUNCATE TABLE stg.Products;
---------------------------------------------
INSERT INTO stg.Products
SELECT P.StockCode, 
       P.Description,
	   CONVERT(NVARCHAR(32),HashBytes('MD5', CONCAT(P.StockCode, P.Description)),2) AS HASH_KEY
FROM src.Products P
LEFT JOIN ProductsDuplicates PD
ON UPPER(P.STOCKCODE) = PD.STOCKCODE
LEFT JOIN ProductsNoDesc PND
ON UPPER(P.STOCKCODE) = PND.STOCKCODE
WHERE PD.STOCKCODE IS NULL
  AND PND.STOCKCODE IS NULL;
---------------------------------------------
