select * from Log.Logs;


create  or alter procedure [Staging].[LoadTransactionFromJson]
AS
BEGIN
    declare @jobname nvarchar(max) = 'RawData_upload_'+ CAST(getdate() as varchar)
    insert into Log.Logs(
    jobname, 
    status
    )
    values(@jobname, 'IN PROGRESS')
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
update log.Logs
set status='SUCCEDED', end_date=getdate() WHERE jobname= @jobname
END
GO

exec [Staging].[LoadTransactionFromJson]
select * from log.Logs

--creez un trigger de try-catch pentru erori:

create  or alter procedure [Staging].[LoadTransactionFromJson]
AS
BEGIN
    begin try
    begin transaction
    declare @jobname nvarchar(max) = 'RawData_upload_'+ CAST(getdate() as varchar)
    insert into Log.Logs(
    jobname, 
    status
    )
    values(@jobname, 'IN PROGRESS')
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
update log.Logs
set status='SUCCEDED', end_date=getdate() WHERE jobname= @jobname
    commit transaction
    end try
        begin  catch
            if @@TRANCOUNT>0
                rollback transaction
            update log.Logs
            set status='FAILED', end_date=getdate() WHERE jobname= @jobname

    end catch
END
GO

exec [Staging].[LoadTransactionFromJson]

select * from log.Logs