USE [fraud_ecommerce]
GO

/****** Object:  Table [Log].[Logs]    Script Date: 12/13/2025 1:22:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [Log].[Logs](
	[logID] [int] IDENTITY(1,1) NOT NULL,
	[jobname] [nvarchar](max) NULL,
	[status] [varchar](50) NULL,
	[output_message] [varchar](50) NULL,
	[start_date] [datetime] NULL,
	[end_date] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[logID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [Log].[Logs] ADD  DEFAULT (getdate()) FOR [start_date]
GO


