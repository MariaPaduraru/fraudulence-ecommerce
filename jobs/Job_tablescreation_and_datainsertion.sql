USE [msdb]
GO

/****** Object:  Job [fradu_comm_with_job1]    Script Date: 11/25/2025 8:07:49 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 11/25/2025 8:07:49 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'fradu_comm_with_job1', 
		@enabled=0, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'Marvan\mantu', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [step1_crearetab]    Script Date: 11/25/2025 8:07:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'step1_crearetab', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''staging.RawData'', ''U'') is not null 
	drop table Staging.RawData
GO
create table staging.RawData ( 
ID int primary key identity(1,1), 
JsonData nvarchar(max), --pt datele din json
LoadDate datetime DEFAULT(getdate())
);
GO

if object_id(''Staging.Transactions'', ''U'') is not null drop table Staging.Transactions; 
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
GO
', 
		@database_name=N'fraud_ecommerce_with_jobs', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [step2_introddatetab]    Script Date: 11/25/2025 8:07:50 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'step2_introddatetab', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'declare @json nvarchar(MAX);
select @json = BulkColumn
from OPENROWSET(BULK ''C:\Users\Public\Downloads\database1py_fraudcomm.json'', SINGLE_CLOB) AS j;
insert into Staging.RawData (JsonData)
values (@json);

select @json = BulkColumn
from OPENROWSET(BULK ''C:\Users\Public\Downloads\database1py_fraudcomm.json'', SINGLE_CLOB) AS j;
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
FROM OPENJSON(@json, ''$'')
WITH (
    TransactionID NVARCHAR(100)          ''$."Transaction ID"'',
    CustomerID NVARCHAR(100)             ''$."Customer ID"'',
    TransactionAmount NVARCHAR(100)      ''$."Transaction Amount"'',
    TransactionDate NVARCHAR(100)        ''$."Transaction Date"'',
    PaymentMethod NVARCHAR(50)           ''$."Payment Method"'',
    ProductCategory NVARCHAR(100)        ''$."Product Category"'',
    Quantity NVARCHAR(10)                ''$."Quantity"'',
    CustomerAge NVARCHAR(10)             ''$."Customer Age"'',
    CustomerLocation NVARCHAR(100)       ''$."Customer Location"'',
    DeviceUsed NVARCHAR(100)             ''$."Device Used"'',
    IPAddress NVARCHAR(100)              ''$."IP Address"'',
    ShippingAddress NVARCHAR(255)        ''$."Shipping Address"'',
    BillingAddress NVARCHAR(255)         ''$."Billing Address"'',
    IsFraudulent NVARCHAR(10)            ''$."Is Fraudulent"'',
    AccountAgeDays NVARCHAR(10)          ''$."Account Age Days"'',
    TransactionHour NVARCHAR(10)         ''$."Transaction Hour"''
);
', 
		@database_name=N'fraud_ecommerce_with_jobs', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'sch1', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=1, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20251124, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'3ef53456-cd84-47b1-b609-e2ac0db0278e'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

