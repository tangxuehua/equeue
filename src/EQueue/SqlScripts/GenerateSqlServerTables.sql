CREATE TABLE [dbo].[message](
    [id] [bigint] NOT NULL,
    [topic] [varchar](128) NOT NULL,
    [queue_id] [int] NOT NULL,
    [queue_offset] [bigint] NOT NULL,
    [body] [varbinary](max) NOT NULL,
    [stored_time] [datetime] NOT NULL,
 CONSTRAINT [PK_message] PRIMARY KEY CLUSTERED 
(
    [id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

CREATE TABLE [dbo].[queue_offset](
    [version] [bigint] NOT NULL,
    [consumer_group] [nvarchar](128) NOT NULL,
    [topic] [nvarchar](128) NOT NULL,
    [queue_id] [int] NOT NULL,
    [queue_offset] [bigint] NOT NULL,
    [timestamp] [datetime] NOT NULL,
 CONSTRAINT [PK_queue_offset_1] PRIMARY KEY CLUSTERED 
(
    [consumer_group] ASC,
    [topic] ASC,
    [queue_id] ASC,
    [version] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO