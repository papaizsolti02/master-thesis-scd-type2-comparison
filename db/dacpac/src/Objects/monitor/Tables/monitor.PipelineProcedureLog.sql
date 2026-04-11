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
    [PipelineRunLogId] BIGINT NOT NULL,
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
    [PageWrites] BIGINT NULL,
    [ErrorNumber] INT NULL,
    [ErrorMessage] NVARCHAR(4000) NULL,
    [CreatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineProcedureLog_CreatedUtc] DEFAULT (SYSUTCDATETIME()),
    [UpdatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineProcedureLog_UpdatedUtc] DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT [PK_monitor_PipelineProcedureLog] PRIMARY KEY CLUSTERED ([Id] ASC),
    CONSTRAINT [UQ_monitor_PipelineProcedureLog_Run_Proc] UNIQUE ([PipelineRunLogId], [ProcedureName]),
    CONSTRAINT [FK_monitor_PipelineProcedureLog_PipelineRunLog] FOREIGN KEY ([PipelineRunLogId])
        REFERENCES [monitor].[PipelineRunLog] ([Id]),
    CONSTRAINT [CK_monitor_PipelineProcedureLog_Status] CHECK ([Status] IN ('Started', 'Succeeded', 'Failed', 'Cancelled')),
    CONSTRAINT [CK_monitor_PipelineProcedureLog_Phase] CHECK ([ProcedurePhase] IN ('Stage', 'Merge', 'Other'))
);
GO

CREATE INDEX [IX_monitor_PipelineProcedureLog_RunLogId]
    ON [monitor].[PipelineProcedureLog] ([PipelineRunLogId]);
GO

CREATE INDEX [IX_monitor_PipelineProcedureLog_ProcedureName_StartUtc]
    ON [monitor].[PipelineProcedureLog] ([ProcedureName], [StartUtc] DESC);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Granular per-procedure metrics for each ADF pipeline run.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the procedure log row.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Foreign key to the pipeline run log surrogate key.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'PipelineRunLogId';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Procedure name captured for the monitoring row.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'ProcedureName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Procedure phase within the pipeline.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'ProcedurePhase';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Procedure start UTC timestamp.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'StartUtc';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Procedure end UTC timestamp.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'EndUtc';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Procedure duration in milliseconds.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'DurationMs';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Procedure execution status.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'Status';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rows read by the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'RowsRead';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rows scanned by the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'RowsScanned';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rows written by the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'RowsWritten';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rows inserted by the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'RowsInserted';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rows updated by the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'RowsUpdated';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rows expired by the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'RowsExpired';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'CPU time consumed by the procedure in milliseconds.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'CpuTimeMs';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Logical reads captured for the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'LogicalReads';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Physical reads captured for the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'PhysicalReads';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Page writes captured for the procedure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'PageWrites';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Error number captured on failure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'ErrorNumber';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Error message captured on failure.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'ErrorMessage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'UTC timestamp when the row was created.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'CreatedUtc';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'UTC timestamp when the row was last updated.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineProcedureLog',
    @level2type = N'COLUMN', @level2name = N'UpdatedUtc';
GO
