USE BetssonDB;
---------------------------------------------
--Get Duplicates Products 
TRUNCATE TABLE ProductsDuplicates;
---------------------------------------------
WITH A AS
(SELECT UPPER(STOCKCODE) AS STOCKCODE, 
        COUNT(*) AS STOCK_CODE_CNT
 FROM SRC.PRODUCTS
 GROUP BY UPPER(STOCKCODE)
 HAVING COUNT(*)>1)

INSERT INTO ProductsDuplicates
SELECT DISTINCT A.STOCKCODE, 
       DESCRIPTION
FROM A
INNER JOIN SRC.PRODUCTS P
ON A.STOCKCODE =  UPPER(P.STOCKCODE)
ORDER BY A.STOCKCODE;
---------------------------------------------
---------------------------------------------
--Get Products with empty Description
TRUNCATE TABLE ProductsNoDesc;
---------------------------------------------
INSERT INTO ProductsNoDesc
SELECT UPPER(STOCKCODE) AS STOCKCODE, 
       DESCRIPTION
FROM SRC.PRODUCTS
WHERE UPPER(STOCKCODE) NOT IN (SELECT DISTINCT STOCKCODE FROM ProductsDuplicates)
  AND DESCRIPTION = '';
