USE BetssonDB;
---------------------------------------------
TRUNCATE TABLE CustomersInvalid;
---------------------------------------------
WITH A AS
(SELECT ISNUMERIC(CUSTOMERID) AS CUS_INT,
        CUSTOMERID
 FROM src.Customers P)

INSERT INTO CustomersInvalid
SELECT CUSTOMERID
FROM A
WHERE CUS_INT = 0;
---------------------------------------------
---------------------------------------------

