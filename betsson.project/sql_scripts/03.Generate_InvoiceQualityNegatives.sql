---------------------------------------------
TRUNCATE TABLE InvoiceQualityNegative;
---------------------------------------------'
INSERT INTO InvoiceQualityNegative
SELECT DISTINCT StockCode, Quantity, Price
FROM src.Invoice
WHERE Quantity < 0
order by StockCode;
