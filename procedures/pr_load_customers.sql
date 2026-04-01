USE [fraud_ecommerce]
GO

/****** Object:  StoredProcedure [dbo].[pr_load_customers]    Script Date: 12/10/2025 4:30:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create   procedure [dbo].[pr_load_customers]
    as
    begin
            -- ultima data procesata in tabelul final
        declare @max_load_date datetime = isnull(
            (select max(c.load_date) from processed.customers as c),
            '1900-01-01'
        );

        insert into processed.customers (customer_id, age, location, account_age_days, load_date)
        select distinct
            s.customerid,
            s.customerage,
            s.customerlocation,
            s.accountagedays,
            s.loaddate
        from staging.transactions as s
        where 
            s.loaddate >= @max_load_date
            and not exists (
                select 1 
                from processed.customers as c
                where c.customer_id = s.customerid
            );
end;
GO


