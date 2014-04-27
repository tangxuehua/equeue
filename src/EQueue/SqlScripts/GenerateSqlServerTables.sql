CREATE TABLE [dbo].[Message](
    [MessageOffset] [bigint] NOT NULL,
    [Topic] [varchar](128) NOT NULL,
    [QueueId] [int] NOT NULL,
    [QueueOffset] [bigint] NOT NULL,
    [Body] [varbinary](max) NOT NULL,
    [StoredTime] [datetime] NOT NULL,
 CONSTRAINT [PK_Message] PRIMARY KEY CLUSTERED 
(
    [MessageOffset] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[QueueOffset](
    [Version] [bigint] NOT NULL,
    [ConsumerGroup] [nvarchar](128) NOT NULL,
    [Topic] [nvarchar](128) NOT NULL,
    [QueueId] [int] NOT NULL,
    [QueueOffset] [bigint] NOT NULL,
    [Timestamp] [datetime] NOT NULL,
 CONSTRAINT [PK_QueueOffset] PRIMARY KEY CLUSTERED 
(
    [ConsumerGroup] ASC,
    [Topic] ASC,
    [QueueId] ASC,
    [Version] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO