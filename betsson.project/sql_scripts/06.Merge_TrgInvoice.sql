MERGE trg.FactInvoice AS Target
    USING stg.Invoice	AS Source
    ON Source.hash_key = Target.hash_key

WHEN NOT MATCHED BY TARGET THEN
    INSERT (invoice, quantity, price, country_sk_id, product_sk_id, customer_sk_id, invoice_date, hash_key, start_date, end_date) 
    VALUES (Source.invoice, Source.quantity, Source.price, Source.country_sk_id, Source.product_sk_id, Source.customer_sk_id, Source.invoice_date, Source.hash_key, GETDATE(), '9999-12-31');
