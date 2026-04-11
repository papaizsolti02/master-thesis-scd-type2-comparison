-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.usp_ProcedureRunFinish
-- Description: Finalizes procedure-level monitoring row with detailed metrics.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [monitor].[usp_ProcedureRunFinish]
    @PipelineRunId NVARCHAR(128),
    @ProcedureName SYSNAME,
    @Status NVARCHAR(20),
    @RowsRead BIGINT = NULL,
    @RowsScanned BIGINT = NULL,
    @RowsWritten BIGINT = NULL,
    @RowsInserted BIGINT = NULL,
    @RowsUpdated BIGINT = NULL,
    @RowsExpired BIGINT = NULL,
    @CpuTimeMs BIGINT = NULL,
    @LogicalReads BIGINT = NULL,
    @PhysicalReads BIGINT = NULL,
    @Writes BIGINT = NULL,
    @ErrorNumber INT = NULL,
    @ErrorMessage NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NowUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @NormalizedProcedureName SYSNAME = NULLIF(TRIM(@ProcedureName), '');
    DECLARE @NormalizedStatus NVARCHAR(20) = NULLIF(TRIM(@Status), '');
    DECLARE @NormalizedErrorMessage NVARCHAR(4000) = NULLIF(TRIM(@ErrorMessage), '');
    DECLARE @PipelineRunLogId BIGINT = NULL;

    IF @NormalizedPipelineRunId IS NULL
    BEGIN
        THROW 51040, 'PipelineRunId is required.', 1;
    END;

    IF @NormalizedProcedureName IS NULL
    BEGIN
        THROW 51041, 'ProcedureName is required.', 1;
    END;

    IF @NormalizedStatus IS NULL
       OR @NormalizedStatus NOT IN ('Succeeded', 'Failed', 'Cancelled')
    BEGIN
        THROW 51042, 'Status must be one of: Succeeded, Failed, Cancelled.', 1;
    END;

    IF @RowsRead IS NOT NULL AND @RowsRead < 0
    BEGIN
        THROW 51043, 'RowsRead cannot be negative.', 1;
    END;

    IF @RowsScanned IS NOT NULL AND @RowsScanned < 0
    BEGIN
        THROW 51044, 'RowsScanned cannot be negative.', 1;
    END;

    IF @RowsWritten IS NOT NULL AND @RowsWritten < 0
    BEGIN
        THROW 51045, 'RowsWritten cannot be negative.', 1;
    END;

    IF @RowsInserted IS NOT NULL AND @RowsInserted < 0
    BEGIN
        THROW 51046, 'RowsInserted cannot be negative.', 1;
    END;

    IF @RowsUpdated IS NOT NULL AND @RowsUpdated < 0
    BEGIN
        THROW 51047, 'RowsUpdated cannot be negative.', 1;
    END;

    IF @RowsExpired IS NOT NULL AND @RowsExpired < 0
    BEGIN
        THROW 51048, 'RowsExpired cannot be negative.', 1;
    END;

    IF @CpuTimeMs IS NOT NULL AND @CpuTimeMs < 0
    BEGIN
        THROW 51049, 'CpuTimeMs cannot be negative.', 1;
    END;

    IF @LogicalReads IS NOT NULL AND @LogicalReads < 0
    BEGIN
        THROW 51050, 'LogicalReads cannot be negative.', 1;
    END;

    IF @PhysicalReads IS NOT NULL AND @PhysicalReads < 0
    BEGIN
        THROW 51051, 'PhysicalReads cannot be negative.', 1;
    END;

    IF @Writes IS NOT NULL AND @Writes < 0
    BEGIN
        THROW 51052, 'Writes cannot be negative.', 1;
    END;

    SELECT @PipelineRunLogId = p.[Id]
    FROM
        [monitor].[PipelineRunLog] AS p
    WHERE
        p.[PipelineRunId] = @NormalizedPipelineRunId;

    IF @PipelineRunLogId IS NULL
    BEGIN
        THROW 51002, 'PipelineRunId not found in monitor.PipelineRunLog. Log pipeline start first.', 1;
    END;

    BEGIN TRY
        UPDATE p
        SET
            p.[EndUtc] = @NowUtc,
            p.[DurationMs] = DATEDIFF_BIG(MILLISECOND, p.[StartUtc], @NowUtc),
            p.[Status] = @NormalizedStatus,
            p.[RowsRead] = @RowsRead,
            p.[RowsScanned] = @RowsScanned,
            p.[RowsWritten] = @RowsWritten,
            p.[RowsInserted] = @RowsInserted,
            p.[RowsUpdated] = @RowsUpdated,
            p.[RowsExpired] = @RowsExpired,
            p.[CpuTimeMs] = @CpuTimeMs,
            p.[LogicalReads] = @LogicalReads,
            p.[PhysicalReads] = @PhysicalReads,
            p.[PageWrites] = @Writes,
            p.[ErrorNumber] = @ErrorNumber,
            p.[ErrorMessage] = @NormalizedErrorMessage,
            p.[UpdatedUtc] = @NowUtc
        FROM
            [monitor].[PipelineProcedureLog] AS p
        WHERE
            p.[PipelineRunLogId] = @PipelineRunLogId
            AND p.[ProcedureName] = @NormalizedProcedureName;

        IF @@ROWCOUNT = 0
        BEGIN
            THROW 51003, 'Procedure row not found in monitor.PipelineProcedureLog. Log procedure start first.', 1;
        END;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO
