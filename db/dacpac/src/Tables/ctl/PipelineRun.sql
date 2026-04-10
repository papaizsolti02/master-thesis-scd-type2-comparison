CREATE TABLE [ctl].[PipelineRun]
(
    [PipelineRunId] BIGINT IDENTITY (1, 1) NOT NULL,
    [BatchId] NVARCHAR(64) NOT NULL,
    [EnvironmentName] NVARCHAR(16) NOT NULL,
    [StartedAt] DATETIME2(3) NOT NULL CONSTRAINT [DF_PipelineRun_StartedAt] DEFAULT SYSUTCDATETIME(),
    [FinishedAt] DATETIME2(3) NULL,
    [Status] NVARCHAR(20) NOT NULL CONSTRAINT [DF_PipelineRun_Status] DEFAULT N'STARTED',
    CONSTRAINT [PK_PipelineRun] PRIMARY KEY CLUSTERED ([PipelineRunId] ASC)
);
GO
