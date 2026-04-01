USE [fraud_ecommerce]
GO

/****** Object:  View [dbo].[vw_fraudulent_high_value_transactions]    Script Date: 1/8/2026 7:20:14 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   view [dbo].[vw_fraudulent_high_value_transactions]
as
select
    t.transaction_amount,
    c.age,
    c.account_age_days,
    c.load_date,
    c.location,
    t.is_fraudulent
from Processed.transactions as t
join Processed.customers as c
    on t.customer_id = c.customer_id
where
    t.is_fraudulent = 1
    and c.age between 20 and 51;
GO


