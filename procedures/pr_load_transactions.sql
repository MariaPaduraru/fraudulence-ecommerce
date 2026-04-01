USE [fraud_ecommerce]
GO

/****** Object:  StoredProcedure [dbo].[pr_load_transactions]    Script Date: 12/10/2025 4:41:01 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   procedure [dbo].[pr_load_transactions]
as
begin
    -- ultima data procesata dupa transaction_date
    declare @max_transaction_date datetime = isnull(
        (select max(t.transaction_date) from processed.transactions as t),
        '1900-01-01'
    );
    insert into processed.transactions (
        customer_id,
        product_id,
        payment_method_id,
        ip_address,
        shipping_address,
        billing_address,
        transaction_amount,
        transaction_date,
        quantity,
        is_fraudulent,
        transaction_hour
    )
    select
        s.customerid,
        p.product_id,
        m.payment_method_id,
        s.ipaddress,
        s.shippingaddress,
        s.billingaddress,
        s.transactionamount,
        s.transactiondate,
        s.quantity,
        s.isfraudulent,
        s.transactionhour
    from staging.transactions as s
    join processed.products as p 
        on p.category = s.productcategory
    join processed.payment_methods as m 
        on m.method_name = s.paymentmethod
    join processed.customers as c 
        on c.customer_id = s.customerid
    where 
        s.loaddate >= @max_transaction_date;
end;
GO


