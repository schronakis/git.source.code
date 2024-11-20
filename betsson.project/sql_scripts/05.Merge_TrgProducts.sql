MERGE trg.DimProducts AS Target
    USING stg.Products	AS Source
    ON Source.stockcode = Target.stockcode and is_active = 1
  
WHEN MATCHED AND (Source.hash_key <> Target.hash_key)
THEN UPDATE SET
    Target.end_date	= GETDATE(),
	Target.is_active = 0;
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
MERGE trg.DimProducts AS Target
    USING stg.Products	AS Source
    ON Source.stockcode = Target.stockcode and is_active = 1

WHEN NOT MATCHED BY TARGET THEN
    INSERT (stockcode, description, hash_key, start_date, end_date, is_active) 
    VALUES (Source.stockcode, Source.description, Source.hash_key, GETDATE(), '9999-12-31', 1);