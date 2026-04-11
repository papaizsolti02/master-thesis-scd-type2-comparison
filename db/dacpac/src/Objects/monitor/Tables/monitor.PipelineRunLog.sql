-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.PipelineRunLog
-- Description: One row per ADF pipeline run for high-level monitoring.
-- Version: 1.0
-- -----------------------------------------------------------------------------
CREATE TABLE [monitor].[PipelineRunLog]
(
    [PipelineRunId] NVARCHAR(128) NOT NULL,
    [PipelineName] NVARCHAR(200) NOT NULL,
    [EnvironmentName] NVARCHAR(20) NULL,
    [TriggerType] NVARCHAR(50) NULL,
    [TriggerName] NVARCHAR(200) NULL,
    [SourceFileName] NVARCHAR(260) NULL,
    [RValue] DECIMAL(12, 6) NULL,
    [CValue] DECIMAL(12, 6) NULL,
    [StartUtc] DATETIME2(3) NOT NULL,
    [EndUtc] DATETIME2(3) NULL,
    [DurationMs] BIGINT NULL,
    [DurationMinutes] DECIMAL(18, 4) NULL,
    [Status] NVARCHAR(20) NOT NULL,
    [RowsRead] BIGINT NULL,
    [RowsWritten] BIGINT NULL,
    [RowsCopied] BIGINT NULL,
    [ErrorCode] NVARCHAR(100) NULL,
    [ErrorMessage] NVARCHAR(4000) NULL,
    [CreatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineRunLog_CreatedUtc] DEFAULT (SYSUTCDATETIME()),
    [UpdatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineRunLog_UpdatedUtc] DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT [PK_monitor_PipelineRunLog] PRIMARY KEY CLUSTERED ([PipelineRunId] ASC),
    CONSTRAINT [CK_monitor_PipelineRunLog_Status] CHECK ([Status] IN ('Started', 'Succeeded', 'Failed', 'Cancelled'))
);
GO
