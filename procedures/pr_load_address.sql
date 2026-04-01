USE [fraud_ecommerce]
GO

/****** Object:  StoredProcedure [dbo].[pr_load_address]    Script Date: 12/10/2025 4:41:27 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   procedure [dbo].[pr_load_address]
as
begin
    -- ultima data procesata in tabelul final
    declare @max_load_date datetime = isnull(
        (select max(a.load_date) from processed.address as a),
        '1900-01-01'
    );

    insert into processed.address (shipping_address, billing_address, load_date)
    select distinct
        s.shippingaddress,
        s.billingaddress,
        s.loaddate
    from staging.transactions as s
    where 
        s.loaddate >= @max_load_date
        and not exists (
            select 1 
            from processed.address as a
            where a.shipping_address = s.shippingaddress
              and a.billing_address = s.billingaddress
        );
end;
GO


