USE [fraud_ecommerce]
GO

/****** Object:  StoredProcedure [dbo].[pr_load_products]    Script Date: 12/10/2025 4:35:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create procedure [dbo].[pr_load_products]
as
begin
    -- ultima data procesata in tabelul final
    declare @max_load_date datetime = isnull(
        (select max(p.load_date) from processed.products as p),
        '1900-01-01'
    );

    insert into processed.products (category, load_date)
    select distinct
        s.productcategory,
        s.loaddate
    from staging.transactions as s
    where 
        s.loaddate >= @max_load_date
        and not exists (
            select 1
            from processed.products as p
            where p.category = s.productcategory
        );
end;
GO


