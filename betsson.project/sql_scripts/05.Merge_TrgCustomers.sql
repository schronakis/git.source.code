MERGE trg.DimCustomers AS Target
    USING stg.Customers	AS Source
    ON Source.customer_id = Target.customer_id and is_active = 1
  
WHEN MATCHED AND (Source.hash_key <> Target.hash_key)
THEN UPDATE SET
    Target.end_date	= GETDATE(),
	Target.is_active = 0;
------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------
MERGE trg.DimCustomers AS Target
    USING stg.Customers	AS Source
    ON Source.customer_id = Target.customer_id and is_active = 1

WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, hash_key, start_date, end_date, is_active) 
    VALUES (Source.customer_id, Source.hash_key, GETDATE(), '9999-12-31', 1);
