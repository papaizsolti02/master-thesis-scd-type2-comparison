CREATE TABLE [ctl].[DeployHistory]
(
    [DeployHistoryId] INT IDENTITY (1, 1) NOT NULL,
    [ScriptPath] NVARCHAR(300) NOT NULL,
    [DeployedAt] DATETIME2(3) NOT NULL CONSTRAINT [DF_DeployHistory_DeployedAt] DEFAULT SYSUTCDATETIME(),
    [DeployedBy] NVARCHAR(128) NULL,
    CONSTRAINT [PK_DeployHistory] PRIMARY KEY CLUSTERED ([DeployHistoryId] ASC)
);
GO
