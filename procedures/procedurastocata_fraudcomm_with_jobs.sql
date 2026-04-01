select * from Staging.RawData
select * from Staging.Transactions

--creez o procedura stocata: nu se fol in select, ci cu exec

create or alter procedure Staging.LoadTransactionFromJson
AS
BEGIN
    declare @json nvarchar(MAX);
    select @json = BulkColumn
    from OPENROWSET(BULK 'C:\Users\Public\Downloads\database1py_fraudcomm.json', SINGLE_CLOB) AS j;-- AM SA O SCRIU O SINGURA DATA CA SA NU IMI CITEASCA FISIERUL JSON DE DOUA ORI-SA FIE EFICIENT
     
        insert into Staging.RawData (JsonData)
        values (@json);

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
        TransactionID NVARCHAR(100)          '$."Transaction ID"',
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
END

--acum vreau sa o execut: 

--prima data sa sterg datele din tabele: 
delete Staging.RawData
delete Staging.Transactions

exec Staging.LoadTransactionFromJson

--dupa exec am mesajul: (1 row affected)
--(23634 rows affected)
--Completion time: 2025-11-25T20:38:22.1843658+02:00