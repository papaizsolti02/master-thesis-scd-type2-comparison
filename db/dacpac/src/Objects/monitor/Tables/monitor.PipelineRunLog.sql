-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.PipelineRunLog
-- Description: One row per ADF pipeline run for high-level monitoring.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE TABLE [monitor].[PipelineRunLog]
(
    [Id] BIGINT IDENTITY (1, 1) NOT NULL,
    [PipelineRunId] NVARCHAR(128) NOT NULL,
    [PipelineName] NVARCHAR(200) NOT NULL,
    [EnvironmentName] NVARCHAR(20) NULL,
    [TriggerType] NVARCHAR(50) NULL,
    [TriggerName] NVARCHAR(200) NULL,
    [SourceFileName] NVARCHAR(260) NULL,
    [SCD2Method] NVARCHAR(100) NULL,
    [RValue] DECIMAL(12, 6) NULL,
    [CValue] DECIMAL(12, 6) NULL,
    [StartUtc] DATETIME2(3) NOT NULL,
    [EndUtc] DATETIME2(3) NULL,
    [DurationMs] BIGINT NULL,
    [DurationMinutes] DECIMAL(18, 4) NULL,
    [Status] NVARCHAR(20) NOT NULL,
    [ErrorCode] NVARCHAR(100) NULL,
    [ErrorMessage] NVARCHAR(4000) NULL,
    [CreatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineRunLog_CreatedUtc] DEFAULT (SYSUTCDATETIME()),
    [UpdatedUtc] DATETIME2(3) NOT NULL CONSTRAINT [DF_monitor_PipelineRunLog_UpdatedUtc] DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT [PK_monitor_PipelineRunLog] PRIMARY KEY CLUSTERED ([Id] ASC),
    CONSTRAINT [UQ_monitor_PipelineRunLog_PipelineRunId] UNIQUE ([PipelineRunId]),
    CONSTRAINT [CK_monitor_PipelineRunLog_Status] CHECK ([Status] IN ('Started', 'Succeeded', 'Failed', 'Cancelled'))
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'One row per ADF pipeline run for high-level monitoring.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the pipeline run log row.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'ADF pipeline run identifier.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'PipelineRunId';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Pipeline name reported by ADF.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'PipelineName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Environment where the pipeline ran.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'EnvironmentName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Trigger type used to start the pipeline.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'TriggerType';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Trigger name used to start the pipeline.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'TriggerName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Source file name associated with the run.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'SourceFileName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Benchmark r parameter captured for the run.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'RValue';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Benchmark c parameter captured for the run.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'CValue';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'SCD type 2 method used by the pipeline run.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'SCD2Method';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Pipeline start UTC timestamp.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'StartUtc';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Pipeline end UTC timestamp.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'EndUtc';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Pipeline duration in milliseconds.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'DurationMs';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Pipeline duration in minutes.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'DurationMinutes';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Pipeline status at the time of logging.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'Status';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Error code captured if the pipeline failed.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'ErrorCode';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Error message captured if the pipeline failed.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'ErrorMessage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'UTC timestamp when the row was created.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'CreatedUtc';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'UTC timestamp when the row was last updated.',
    @level0type = N'SCHEMA', @level0name = N'monitor',
    @level1type = N'TABLE', @level1name = N'PipelineRunLog',
    @level2type = N'COLUMN', @level2name = N'UpdatedUtc';
GO
