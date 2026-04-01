-- creeaza tabel temporar local cu tranzactii recente
create table #temp_local (
    transaction_id int,
    customer_id char(36),
    transaction_amount decimal(10,2),
    transaction_date datetime
);

-- inserare date din tabelul real
insert into #temp_local (transaction_id, customer_id, transaction_amount, transaction_date)
select top 10 
    transaction_id,
    customer_id,
    transaction_amount,
    transaction_date
from Processed.transactions
order by transaction_amount DESC;

-- vizualizare
select * from #temp_local;