select * from Processed.transactions

create table processed.transactionaudit (
    auditid int identity(1,1) primary key,
    transactionid int,
    customerid char(36),
    productid int,
    transactionamount decimal(10,2),
    transactiondate datetime,
    isfraudulent bit,
    actiontype varchar(10),
    actiondate datetime default getdate()
);

create trigger trigger_transactionaudit
on processed.transactions
after insert, update, delete
as
begin
    -- pentru insert și update
    insert into processed.transactionaudit 
        (transactionid, customerid, productid, transactionamount, transactiondate, isfraudulent, actiontype)
    select 
        transaction_id,
        customer_id,
        product_id,
        transaction_amount,
        transaction_date,
        is_fraudulent,
        case 
            when exists(select 1 from inserted) and not exists(select 1 from deleted) then 'INSERT' --Dacă inserted există și deleted nu există → e INSERT
            when exists(select 1 from inserted) and exists(select 1 from deleted) then 'UPDATE'--Dacă există ambele → e UPDATE-> cand fac UPDATE imi insereaza noua valoare dar o adauga in deleted pe cea veche
        end
    from inserted;

    -- pentru delete
    insert into processed.transactionaudit 
        (transactionid, customerid, productid, transactionamount, transactiondate, isfraudulent, actiontype)
    select 
        transaction_id,
        customer_id,
        product_id,
        transaction_amount,
        transaction_date,
        is_fraudulent,
        'DELETE'
    from deleted;
end;

select * from  processed.transactionaudit 
-- arată toate triggerele definite pe tabelul 'transactions'
select name, object_id, parent_class_desc, type_desc
from sys.triggers
where parent_id = object_id('processed.transactions');


--inserez date in tabelul de processed.transaction: 
insert into processed.transactions
(customer_id, product_id, transaction_amount, ip_address, transaction_date, is_fraudulent, transaction_hour, payment_method_id,quantity)
values
('test_customer_01', 6, 100, 111, getdate(), 1, datepart(hour, getdate()), 1, 1);


select top 5 customer_id from processed.customers;

select * from Processed.devices
select * from Processed.transactions

insert into Processed.devices(device_type, ip_address, load_date)
values('test_type','111', getdate())

insert into processed.customers (customer_id, age, location, account_age_days, load_date)
values ('test_customer_01', 33, 'location_test', 10, getdate());

select* from Processed.products

insert into Processed.products(category, load_date)
values ( 'test_category', getdate())


select * from Processed.customers

select * from  processed.transactionaudit 