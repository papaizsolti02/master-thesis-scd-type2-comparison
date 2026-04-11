-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.usp_ProcedureRunStart
-- Description: Creates or resets procedure-level monitoring row at proc start.
-- Version: 1.0
-- -----------------------------------------------------------------------------
CREATE PROCEDURE [monitor].[usp_ProcedureRunStart]
    @PipelineRunId NVARCHAR(128),
    @ProcedureName SYSNAME,
    @ProcedurePhase NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NowUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @NormalizedProcedureName SYSNAME = NULLIF(TRIM(@ProcedureName), '');
    DECLARE @NormalizedProcedurePhase NVARCHAR(20) = NULLIF(TRIM(@ProcedurePhase), '');

    IF @NormalizedPipelineRunId IS NULL
    BEGIN
        THROW 51030, 'PipelineRunId is required.', 1;
    END;

    IF @NormalizedProcedureName IS NULL
    BEGIN
        THROW 51031, 'ProcedureName is required.', 1;
    END;

    IF @NormalizedProcedurePhase IS NULL
       OR @NormalizedProcedurePhase NOT IN ('Stage', 'Merge', 'Other')
    BEGIN
        THROW 51032, 'ProcedurePhase must be one of: Stage, Merge, Other.', 1;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM
            [monitor].[PipelineRunLog]
        WHERE
            [PipelineRunId] = @NormalizedPipelineRunId
    )
    BEGIN
        THROW 51002, 'PipelineRunId not found in monitor.PipelineRunLog. Log pipeline start first.', 1;
    END;

    BEGIN TRY
        BEGIN TRAN;

        UPDATE p
        SET
            p.[ProcedurePhase] = @NormalizedProcedurePhase,
            p.[StartUtc] = @NowUtc,
            p.[EndUtc] = NULL,
            p.[DurationMs] = NULL,
            p.[Status] = 'Started',
            p.[RowsRead] = NULL,
            p.[RowsScanned] = NULL,
            p.[RowsWritten] = NULL,
            p.[RowsInserted] = NULL,
            p.[RowsUpdated] = NULL,
            p.[RowsExpired] = NULL,
            p.[CpuTimeMs] = NULL,
            p.[LogicalReads] = NULL,
            p.[PhysicalReads] = NULL,
            p.[Writes] = NULL,
            p.[ErrorNumber] = NULL,
            p.[ErrorMessage] = NULL,
            p.[UpdatedUtc] = @NowUtc
        FROM [monitor].[PipelineProcedureLog] AS p WITH (UPDLOCK, HOLDLOCK)
        WHERE p.[PipelineRunId] = @NormalizedPipelineRunId
            AND p.[ProcedureName] = @NormalizedProcedureName;

        IF @@ROWCOUNT = 0
        BEGIN
            INSERT INTO [monitor].[PipelineProcedureLog]
            (
                [PipelineRunId],
                [ProcedureName],
                [ProcedurePhase],
                [StartUtc],
                [Status],
                [UpdatedUtc]
            )
            VALUES
            (
                @NormalizedPipelineRunId,
                @NormalizedProcedureName,
                @NormalizedProcedurePhase,
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
