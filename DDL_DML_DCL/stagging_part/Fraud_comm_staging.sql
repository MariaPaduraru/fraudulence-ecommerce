use fraud_ecommerce;

create table staging.RawData ( 
ID int primary key identity(1,1), 
JsonData nvarchar(max), --pt datele din json
LoadDate datetime DEFAULT(getdate())
)
;

--transform datele pe care le am in csv in fisier de tip json + adaugare in tabelul de RawData
--tabelul pentru string date RawData l-am creat in schema de processed si trebuie sa il schimb in schema de string 

CREATE SCHEMA Staging

;
alter table Staging.Transactions
add TransactionDateOnly DATE;

UPDATE Staging.Transactions
set TransactionDateOnly = TRY_CONVERT(DATE, TransactionDate, 23);



select * from Staging.RawData
select* from Staging.Transactions

--delete from Staging.RawData
--where id=3

--Păstrez Staging.RawData ca tabel “brut” (raw landing). Aici incarc doar JSON-ul integral (fără să îl sparg pe coloane).

--Ulterior, creez un al doilea tabel de staging curat, Staging.Transactions, cu coloane pentru fiecare câmp din JSON.

--Apoi scriu: INSERT INTO Staging.Transactions (...) SELECT ... FROM OPENJSON(RawData.JsonData) sau echivalent (în funcție de SQL-ul folosit).

-- Avantaje: Păstrez copia brută a datelor (bună pentru audit/debug). Separ clar landing zone de clean staging.Evit modificarea unui tabel deja folosit în alte procese.


--import datele din sursa externa: 

/*
* REGION JSON
*/
-- import json


--al doilea tabel de staging curat, Staging.Transactions, cu coloane pentru fiecare câmp din JSON:

if object_id('Staging.Transactions', 'U') is not null drop table Staging.Transactions; --daca tab exista sa ii dea drop la tabel --o varianta buna daca nu ma intereseaza consistenta datelor 

create table Staging.Transactions (
    TransactionID nvarchar(100),
    CustomerID nvarchar(100),
    TransactionAmount decimal(18,2),
    TransactionDate DATETIME,
    PaymentMethod nvarchar(50),
    ProductCategory nvarchar(100),
    Quantity int,
    CustomerAge int,
    CustomerLocation nvarchar(100),
    DeviceUsed nvarchar(100),
    IPAddress nvarchar(100),
    ShippingAddress nvarchar(255),
    BillingAddress nvarchar(255),
    IsFraudulent BIT,
    AccountAgeDays INT,
    TransactionHour INT,
    LoadDate DATETIME DEFAULT getdate()
    )
    ;
    --acum iau datele din fisierul json si le incarc in RawData: 

declare @json nvarchar(MAX);

select @json = BulkColumn
from OPENROWSET(BULK 'C:\Users\Public\Downloads\database1py_fraudcomm.json', SINGLE_CLOB) AS j;
--insert into Staging.RawData (JsonData)
--values (@json);

insert into Staging.Transactions (
    TransactionID,
    CustomerID,
    TransactionAmount,
    TransactionDate,
    PaymentMethod,
    ProductCategory,
    Quantity,
    CustomerAge,
    CustomerLocation,
    DeviceUsed,
    IPAddress,
    ShippingAddress,
    BillingAddress,
    IsFraudulent,
    AccountAgeDays,
    TransactionHour
    -- LoadDate NU se pune aici !!
)
select
    TransactionID,
    CustomerID,
    TRY_CONVERT(DECIMAL(18,2), TransactionAmount),
    TRY_CONVERT(DATETIME, TransactionDate),
    PaymentMethod,
    ProductCategory,
    TRY_CONVERT(INT, Quantity),
    TRY_CONVERT(INT, CustomerAge),
    CustomerLocation,
    DeviceUsed,
    IPAddress,
    ShippingAddress,
    BillingAddress,
    TRY_CONVERT(BIT, IsFraudulent),
    TRY_CONVERT(INT, AccountAgeDays),
    TRY_CONVERT(INT, TransactionHour)
FROM OPENJSON(@json, '$')
WITH (
    TransactionID NVARCHAR(100)          '$."Transaction ID"', --    am scris asa pentru ca in json eu am spatii intre 'Transaction'& 'ID' & JSON PATH NU POATE CONȚINE SPAȚII
    CustomerID NVARCHAR(100)             '$."Customer ID"',
    TransactionAmount NVARCHAR(100)      '$."Transaction Amount"',
    TransactionDate NVARCHAR(100)        '$."Transaction Date"',
    PaymentMethod NVARCHAR(50)           '$."Payment Method"',
    ProductCategory NVARCHAR(100)        '$."Product Category"',
    Quantity NVARCHAR(10)                '$."Quantity"',
    CustomerAge NVARCHAR(10)             '$."Customer Age"',
    CustomerLocation NVARCHAR(100)       '$."Customer Location"',
    DeviceUsed NVARCHAR(100)             '$."Device Used"',
    IPAddress NVARCHAR(100)              '$."IP Address"',
    ShippingAddress NVARCHAR(255)        '$."Shipping Address"',
    BillingAddress NVARCHAR(255)         '$."Billing Address"',
    IsFraudulent NVARCHAR(10)            '$."Is Fraudulent"',
    AccountAgeDays NVARCHAR(10)          '$."Account Age Days"',
    TransactionHour NVARCHAR(10)         '$."Transaction Hour"'
);

select * from Staging.Transactions;

--select GETDATE() 

select * from Staging.RawData;

--select  
--    JSON_VALUE(j.value, '$."Transaction ID"') AS TransactionID,
--    JSON_VALUE(j.value, '$."Customer ID"') AS CustomerID,
--    JSON_VALUE(j.value, '$."Transaction Amount"') AS TransactionAmount,
--    JSON_VALUE(j.value,'$."Transaction Date"') AS TransactionDate,
--    JSON_VALUE(j.value,  '$."Payment Method"') AS PaymentMethod,
--    JSON_VALUE(j.value, '$."Product Category"') AS ProductCategory,
--    JSON_VALUE(j.value, '$."Quantity"') AS Quantity,
--    JSON_VALUE(j.value,  '$."Customer Age"') AS CustomerAge,
--    JSON_VALUE(j.value, '$."Customer Location"') AS CustomerLocation,
--    JSON_VALUE(j.value, '$."Device Used"') AS DeviceUsed
--from Staging.RawData r
--CROSS APPLY OPENJSON(r.JsonData) j -- aici am verificat: cu CROSS APPLY am scos fiecare obiect din Json si transforma in cate un rand din SQL 

/*
* ENDREGION
*/

drop table Staging.Transactions;
drop table Staging.RawData;