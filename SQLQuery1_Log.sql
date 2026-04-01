create schema Log;

create table Log.Logs(
logID int primary key identity (1,1),
jobname nvarchar(max),
status varchar(50), 
output_message varchar(50),
start_date datetime DEFAULT(getdate()), 
end_date datetime
)

--
