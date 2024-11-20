USE BetssonDB;
-----------------------------------------------------
WITH DateSequence AS (
    SELECT CAST('2000-01-01' AS DATE) AS date_dt
    UNION ALL
    SELECT DATEADD(DAY, 1, date_dt)
    FROM DateSequence
    WHERE date_dt < '2030-12-31')

INSERT INTO trg.DimTime (date_dt, week_number, year, month, day)
SELECT 
    date_dt,
    DATEPART(WEEK, date_dt) AS week_number,
    DATEPART(YEAR, date_dt) AS year,
    DATEPART(MONTH, date_dt) AS month,
    DATEPART(DAY, date_dt) AS day
FROM DateSequence
OPTION (MAXRECURSION 0);