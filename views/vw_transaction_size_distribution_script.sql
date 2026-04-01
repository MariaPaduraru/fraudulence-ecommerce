create or alter view vw_transaction_size_distribution
as
with transactioncategories as (
    select
        case
            when transaction_amount < 100 then 'small'
            when transaction_amount between 100 and 500 then 'medium'
            else 'large'
        end as TransactionSize
    from Processed.transactions
)
select
    TransactionSize,
    count(*) as NumberOfTransactions,
    cast(
        count(*) * 100.0 / sum(count(*)) over ()
        as decimal(5,2)
    ) as PercentageFromTotal
from transactioncategories
group by TransactionSize;

select *
from vw_transaction_size_distribution
order by NumberOfTransactions desc;

