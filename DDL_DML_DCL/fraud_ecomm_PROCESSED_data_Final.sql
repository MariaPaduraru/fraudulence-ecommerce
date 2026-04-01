select * from Staging.Transactions

USE fraud_ecommerce
--mut tabelele din dbo. in Processed:
create schema Processed;

alter schema Processed
transfer dbo.Address;

alter schema Processed
transfer dbo.customers;

alter schema Processed
transfer dbo.devices;

alter schema Processed
transfer dbo.payment_methods;

alter schema Processed
transfer dbo.transactions;

alter schema Processed
transfer dbo.products;

--trebuie sa adaug la fiecare tabel din zona de PROCESSED load_date IN AFARA DE Transactions

alter table Processed.Address
add load_date datetime default getdate()

alter table Processed.customers
add load_date datetime default getdate()

alter table Processed.devices
add load_date datetime default getdate()

alter table Processed.payment_methods
add load_date datetime default getdate()

alter table Processed.products
add load_date datetime default getdate()

--pt tabelul de Processed.Address: 

select * from Processed.address
select * from Staging.Transactions

insert into Processed.Address (shipping_address, billing_address)
select DISTINCT ShippingAddress, 
				BillingAddress
from STAGING.Transactions as S --LUAM load_date din pricessed.category
WHERE S.LoadDate>= ISNULL((select MAX(a.load_date) from Processed.Address as a),CONVERT(datetime, '1900-01-01'));--imi da null - nu pot verifica null cu ceva, asadar folosim Cualesce 

--pt tabelul de Processed.customers: 

select * from Processed.customers;
select * from Staging.Transactions;

insert into Processed.customers (customer_id, age, location, account_age_days)
select DISTINCT CustomerID, 
				CustomerAge, 
				CustomerLocation, 
				AccountAgeDays
from Staging.Transactions as s
WHERE s.LoadDate >= ISNULL((select MAX(c.load_date) from Processed.customers AS c), '1900-01-01') -- imi ia doar datele noi 
	AND NOT EXISTS( select 1 from Processed.customers as c WHERE c.customer_id=s.CustomerID) -- aici evit duplicatele 

--pt tabelul de Processed.devices: 

select * from Processed.devices
select * from Staging.Transactions

--insert into Processed.devices (device_type, ip_address)
--select DISTINCT DeviceUsed, 
--				IPAddress
--from Staging.Transactions as s 
--WHERE s.LoadDate >= ISNULL((SELECT MAX(d.load_date) FROM Processed.devices AS d), '1900-01-01')
--AND NOT EXISTS ( select 1 from Processed.devices as d WHERE d.ip_address=s.IPAddress) -- evită să mai insereze același IP deja existent


--ERROR: Msg 2627, Level 14, State 1, Line 71: Dacă în Staging.Transactions același IPAddress apare asociat cu mai multe DeviceUsed,
--											DISTINCT permite toate combinațiile, dar UNIQUE pe ip_address nu permite duplicatul → eroare.
--Violation of UNIQUE KEY constraint 'UQ__devices__0A4D92D64B5623EE': În tabelul Processed.Devices există o restricție UNIQUE pe coloana ip_address
--Cannot insert duplicate key in object 'Processed.devices': Încerci să inserezi o valoare care deja există sau care ar încălca regula de unicitate
--The duplicate key value is (desktop).

SELECT IPAddress, MIN(DeviceUsed) AS DeviceUsed
FROM Staging.Transactions
GROUP BY IPAddress

--insert into Processed.devices (device_type, ip_address)
--select	MIN(DeviceUsed) AS DeviceUsed,
--		IPAddress
--from Staging.Transactions as s 
--WHERE s.LoadDate >= ISNULL((SELECT MAX(d.load_date) FROM Processed.devices AS d), '1900-01-01')
--AND NOT EXISTS ( select 1 from Processed.devices as d WHERE d.ip_address=s.IPAddress)
--GROUP BY IPAddress
--NOT EXISTS se evaluează pentru fiecare rând din Staging, înainte de GROUP BY .Dacă în Staging există mai multe rânduri cu același IP, toate trec NOT EXISTS check-ul → GROUP BY se face mai târziu
--La momentul INSERT, SQL încearcă să insereze aceleași IP de mai multe ori → conflict UNIQUE


;with uniqueips as (
    select IPAddress, 
			min(DeviceUsed) as DevicesUsed
    from Staging.Transactions as s 
    group by IPAddress
)
insert into Processed.devices (device_type, ip_address)
select u.DevicesUsed, u.IPAddress
from uniqueips  as u
where u.IPAddress not in (select ip_address from processed.devices);


insert into Processed.devices( device_type,ip_address, load_date)
select DISTINCT s.DeviceUsed, 
                s.IPAddress, 
                s.LoadDate
from Staging.Transactions as s 
WHERE s.LoadDate >= ISNULL((select MAX(d.load_date) from Processed.devices AS d), '1900-01-01') -- imi ia doar datele noi 
	AND NOT EXISTS( select 1 from Processed.devices as d WHERE d.device_type=s.DeviceUsed AND d.ip_address=s.IPAddress)

--scoatem constrangerea de UNIQUE pentru device_type pentru ca nu am nevoie de ea : 

ALTER TABLE Processed.devices
DROP CONSTRAINT UQ_device_type_ip;

ALTER TABLE Processed.devices
DROP CONSTRAINT UQ__devices__0A4D92D65D969566;




--pt tabelul de Processed.payment_methods: 

select * from Processed.payment_methods
select * from Staging.Transactions

insert into Processed.payment_methods(method_name, load_date)
select DISTINCT PaymentMethod, 
				LoadDate
from Staging.Transactions as s
WHERE s.LoadDate>=ISNULL((select MAX(p.load_date) from  Processed.payment_methods as p), '1900-01-01') -- imi ia doar datele noi 
	AND NOT EXISTS( select 1 from  Processed.payment_methods as p WHERE p.method_name=s.PaymentMethod)
;

--cate metode unice sunt in Staging: 

select paymentmethod, COUNT(*) AS total
from Staging.Transactions
GROUP BY paymentmethod
ORDER BY total DESC;


--pt tabelul de Processed.products
select * from Processed.products
select * from Staging.Transactions


insert into Processed.products(category, load_date)
select DISTINCT ProductCategory,
				LoadDate
from Staging.Transactions as s 
WHERE s.LoadDate>=ISNULL((select MAX(p.load_date) from  Processed.products as p), '1900-01-01') -- imi ia doar datele noi 
	AND NOT EXISTS( select 1 from  Processed.products as p WHERE p.category=s.ProductCategory)


--cate categorii unice exista in staging
select productcategory, COUNT(*) AS total
from Staging.Transactions
GROUP BY productcategory
ORDER BY total DESC;


--pt tabelul de Processed.Transactions: 

select * from Processed.Transactions

insert into Processed.Transactions(
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
from Staging.Transactions as s 
JOIN Processed.products as p ON p.category=s.ProductCategory
JOIN Processed.payment_methods as m ON s.PaymentMethod=m.method_name
JOIN Processed.customers as c ON c.customer_id=s.CustomerID
WHERE s.LoadDate >= ISNULL((SELECT MAX(t.transaction_date) FROM Processed.transactions AS t), '1900-01-01');

--creez procedura pt tab de processed.customers: 

create or alter procedure [dbo].[pr_load_customers]
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

exec [dbo].[pr_load_customers]
--(0 rows affected) pt ca nu am date noi

select max(load_date) 
from processed.customers;--2025-11-26 21:05:59.500

--procedura pentru payment_methods: 

create or alter procedure [dbo].[pr_load_payment_methods]
as
begin
    -- ultima data procesata in tabelul final
    declare @max_load_date datetime = isnull(
        (select max(p.load_date) from processed.payment_methods as p),
        '1900-01-01'
    );

    insert into processed.payment_methods (method_name, load_date)
    select distinct
        s.paymentmethod,
        s.loaddate
    from staging.transactions as s
    where 
        s.loaddate >= @max_load_date
        and not exists (
            select 1 
            from processed.payment_methods as p
            where p.method_name = s.paymentmethod
        );
end;
GO

--procedura pentru products:

create or alter procedure [dbo].[pr_load_products]
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

--procedura pentru Transactions: 

create or alter procedure [dbo].[pr_load_transactions]
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

--procedura pentru Address:

create or alter procedure [dbo].[pr_load_address]
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


--procedura pentru devices:

create or alter procedure [dbo].[pr_load_devices]
as
begin
    -- ultima data procesata in tabelul final
    declare @max_load_date datetime = isnull(
        (select max(d.load_date) from processed.devices as d),
        '1900-01-01'
    );

    insert into processed.devices (device_type, ip_address, load_date)
    select distinct
        s.deviceused,
        s.ipaddress,
        s.loaddate
    from staging.transactions as s
    where 
        s.loaddate >= @max_load_date
        and not exists (
            select 1 
            from processed.devices as d
            where d.device_type = s.deviceused
              and d.ip_address = s.ipaddress
        );
end;
GO
