-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.PipelineProcedureLog
-- Description: Granular per-procedure metrics for each ADF pipeline run.
-- Version: 1.0
-- -----------------------------------------------------------------------------
CREATE TABLE [monitor].[PipelineProcedureLog]
(
    [Id] BIGINT IDENTITY (1, 1) NOT NULL,
    [PipelineRunId] NVARCHAR(128) NOT NULL,
    [ProcedureName] SYSNAME NOT NULL,
    [ProcedurePhase] NVARCHAR(20) NOT NULL,
    [StartUtc] DATETIME2(3) NOT NULL,
    [EndUtc] DATETIME2(3) NULL,
    [DurationMs] BIGINT NULL,
    [Status] NVARCHAR(20) NOT NULL,
    [RowsRead] BIGINT NULL,
    [RowsScanned] BIGINT NULL,
    [RowsWritten] BIGINT NULL,
    [RowsInserted] BIGINT NULL,
    [RowsUpdated] BIGINT NULL,
    [RowsExpired] BIGINT NULL,
    [CpuTimeMs] BIGINT NULL,
    [LogicalReads] BIGINT NULL,
    [PhysicalReads] BIGINT NULL,
    [Writes] BIGINT NULL,
    [ErrorNumber] INT NULL,
    [ErrorMessage] NVARCHAR(4000) NULL,
    [CreatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineProcedureLog_CreatedUtc] DEFAULT (SYSUTCDATETIME()),
    [UpdatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineProcedureLog_UpdatedUtc] DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT [PK_monitor_PipelineProcedureLog] PRIMARY KEY CLUSTERED ([Id] ASC),
    CONSTRAINT [UQ_monitor_PipelineProcedureLog_Run_Proc] UNIQUE ([PipelineRunId], [ProcedureName]),
    CONSTRAINT [FK_monitor_PipelineProcedureLog_PipelineRunLog] FOREIGN KEY ([PipelineRunId])
        REFERENCES [monitor].[PipelineRunLog] ([PipelineRunId]),
    CONSTRAINT [CK_monitor_PipelineProcedureLog_Status] CHECK ([Status] IN ('Started', 'Succeeded', 'Failed', 'Cancelled')),
    CONSTRAINT [CK_monitor_PipelineProcedureLog_Phase] CHECK ([ProcedurePhase] IN ('Stage', 'Merge', 'Other'))
);
GO

CREATE INDEX [IX_monitor_PipelineProcedureLog_RunId]
    ON [monitor].[PipelineProcedureLog] ([PipelineRunId]);
GO

CREATE INDEX [IX_monitor_PipelineProcedureLog_ProcedureName_StartUtc]
    ON [monitor].[PipelineProcedureLog] ([ProcedureName], [StartUtc] DESC);
GO
