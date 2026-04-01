USE [fraud_ecommerce]
GO

SET ANSI_PADDING ON
GO

/****** Object:  Index [IX_transactions_ip_trdate]    Script Date: 1/12/2026 11:42:17 PM ******/
CREATE NONCLUSTERED INDEX [IX_transactions_ip_trdate] ON [Processed].[transactions]
(
	[ip_address] ASC,
	[transaction_date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO


