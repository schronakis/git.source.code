---------------------------------------------
TRUNCATE TABLE CountriesInvalid;
---------------------------------------------
WITH valid_countries AS
(SELECT distinct
		case when Country = 'U.K.'
		     then 'United Kingdom'
			 when Country = 'EIRE'
			 then 'Ireland'
			 when Country = 'USA'
			 then 'United States of America'
			 when Country = 'RSA'
			 then 'South Africa'
			 else Country
		end as country_name,
		Country
FROM src.Invoice S)

INSERT INTO CountriesInvalid
SELECT distinct 
	country as country_name 
FROM valid_countries S
LEFT JOIN TRG.DimCountries T
ON S.country_name = t.country_name 
WHERE sk_id IS NULL;