---------------------------------------------
TRUNCATE TABLE InvoicePriceNegative;
---------------------------------------------'
INSERT INTO InvoicePriceNegative
SELECT DISTINCT StockCode, Quantity, Price
FROM src.Invoice
WHERE Price < 0
order by StockCode;
