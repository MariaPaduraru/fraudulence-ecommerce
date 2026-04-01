USE [fraud_ecommerce]
GO

/****** Object:  UserDefinedFunction [dbo].[function_transactions_last_7_days]    Script Date: 12/13/2025 7:16:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   function [dbo].[function_transactions_last_7_days](
@nr_days int
)
returns table
as
return
(
   select *
    from Processed.transactions
    where transaction_date >= dateadd(day, -7,(select max(transaction_date) from Processed.transactions)
    )
);
GO


