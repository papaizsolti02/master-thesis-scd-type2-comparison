-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.usp_PipelineRunStart
-- Description: Creates or resets pipeline-level monitoring row at run start.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [monitor].[usp_PipelineRunStart]
    @PipelineRunId NVARCHAR(128),
    @PipelineName NVARCHAR(200),
    @EnvironmentName NVARCHAR(20) = NULL,
    @TriggerType NVARCHAR(50) = NULL,
    @TriggerName NVARCHAR(200) = NULL,
    @SourceFileName NVARCHAR(260) = NULL,
    @RValue DECIMAL(12, 6) = NULL,
    @CValue DECIMAL(12, 6) = NULL,
    @SCD2Method NVARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NowUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @NormalizedPipelineName NVARCHAR(200) = NULLIF(TRIM(@PipelineName), '');
    DECLARE @NormalizedEnvironmentName NVARCHAR(20) = NULLIF(TRIM(@EnvironmentName), '');
    DECLARE @NormalizedTriggerType NVARCHAR(50) = NULLIF(TRIM(@TriggerType), '');
    DECLARE @NormalizedTriggerName NVARCHAR(200) = NULLIF(TRIM(@TriggerName), '');
    DECLARE @NormalizedSourceFileName NVARCHAR(260) = NULLIF(TRIM(@SourceFileName), '');
    DECLARE @NormalizedSCD2Method NVARCHAR(50) = NULLIF(TRIM(@SCD2Method), '');

    IF @NormalizedPipelineRunId IS NULL

    IF @NormalizedPipelineName IS NULL
    BEGIN
        THROW 51011, 'PipelineName is required.', 1;
    END;

    IF @NormalizedEnvironmentName IS NOT NULL
       AND @NormalizedEnvironmentName NOT IN ('Development', 'Production', 'Testing', 'QA')
    BEGIN
        THROW 51012, 'EnvironmentName must be one of: Development, Production, Testing, QA.', 1;
    END;

    BEGIN TRY
        BEGIN TRAN;

        UPDATE p
        SET
            p.[PipelineName] = @NormalizedPipelineName,
            p.[EnvironmentName] = @NormalizedEnvironmentName,
            p.[TriggerType] = @NormalizedTriggerType,
            p.[TriggerName] = @NormalizedTriggerName,
            p.[SourceFileName] = @NormalizedSourceFileName,
            p.[RValue] = @RValue,
            p.[CValue] = @CValue,
            p.[SCD2Method] = @NormalizedSCD2Method,
            p.[StartUtc] = @NowUtc,
            p.[EndUtc] = NULL,
            p.[DurationMs] = NULL,
            p.[DurationMinutes] = NULL,
            p.[Status] = 'Started',
            p.[ErrorCode] = NULL,
            p.[ErrorMessage] = NULL,
            p.[UpdatedUtc] = @NowUtc
        FROM [monitor].[PipelineRunLog] AS p WITH (UPDLOCK, HOLDLOCK)
        WHERE p.[PipelineRunId] = @NormalizedPipelineRunId;

        IF @@ROWCOUNT = 0
        BEGIN
            INSERT INTO [monitor].[PipelineRunLog]
            (
                [PipelineRunId],
                [PipelineName],
                [EnvironmentName],
                [TriggerType],
                [TriggerName],
                [SourceFileName],
                [RValue],
                [CValue],
                [SCD2Method],
                [StartUtc],
                [Status],
                [UpdatedUtc]
            )
            VALUES
            (
                @NormalizedPipelineRunId,
                @NormalizedPipelineName,
                @NormalizedEnvironmentName,
                @NormalizedTriggerType,
                @NormalizedTriggerName,
                @NormalizedSourceFileName,
                @RValue,
                @CValue,
                @NormalizedSCD2Method,
                @NowUtc,
                'Started',
                @NowUtc
            );
        END;

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRAN;
        END;

        THROW;
    END CATCH;
END;
GO
