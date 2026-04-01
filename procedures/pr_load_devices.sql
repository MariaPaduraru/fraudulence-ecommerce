USE [fraud_ecommerce]
GO

/****** Object:  StoredProcedure [dbo].[pr_load_devices]    Script Date: 12/10/2025 6:32:01 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   procedure [dbo].[pr_load_devices]
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


