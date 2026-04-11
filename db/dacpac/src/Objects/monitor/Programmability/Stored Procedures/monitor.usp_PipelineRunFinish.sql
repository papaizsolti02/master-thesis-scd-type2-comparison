-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: monitor.usp_PipelineRunFinish
-- Description: Finalizes pipeline-level monitoring row with status and metrics.
-- Version: 1.0
-- -----------------------------------------------------------------------------
CREATE PROCEDURE [monitor].[usp_PipelineRunFinish]
    @PipelineRunId NVARCHAR(128),
    @Status NVARCHAR(20),
    @RowsRead BIGINT = NULL,
    @RowsWritten BIGINT = NULL,
    @RowsCopied BIGINT = NULL,
    @ErrorCode NVARCHAR(100) = NULL,
    @ErrorMessage NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NowUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @NormalizedStatus NVARCHAR(20) = NULLIF(TRIM(@Status), '');
    DECLARE @NormalizedErrorCode NVARCHAR(100) = NULLIF(TRIM(@ErrorCode), '');
    DECLARE @NormalizedErrorMessage NVARCHAR(4000) = NULLIF(TRIM(@ErrorMessage), '');

    IF @NormalizedPipelineRunId IS NULL
    BEGIN
        THROW 51020, 'PipelineRunId is required.', 1;
    END;

    IF @NormalizedStatus IS NULL
       OR @NormalizedStatus NOT IN ('Succeeded', 'Failed', 'Cancelled')
    BEGIN
        THROW 51021, 'Status must be one of: Succeeded, Failed, Cancelled.', 1;
    END;

    IF @RowsRead IS NOT NULL AND @RowsRead < 0
    BEGIN
        THROW 51022, 'RowsRead cannot be negative.', 1;
    END;

    IF @RowsWritten IS NOT NULL AND @RowsWritten < 0
    BEGIN
        THROW 51023, 'RowsWritten cannot be negative.', 1;
    END;

    IF @RowsCopied IS NOT NULL AND @RowsCopied < 0
    BEGIN
        THROW 51024, 'RowsCopied cannot be negative.', 1;
    END;

    BEGIN TRY
        UPDATE p
        SET
            p.[EndUtc] = @NowUtc,
            p.[DurationMs] = DATEDIFF_BIG(MILLISECOND, p.[StartUtc], @NowUtc),
            p.[DurationMinutes] = CAST(DATEDIFF_BIG(MILLISECOND, p.[StartUtc], @NowUtc) / 60000.0 AS DECIMAL(18, 4)),
            p.[Status] = @NormalizedStatus,
            p.[RowsRead] = @RowsRead,
            p.[RowsWritten] = @RowsWritten,
            p.[RowsCopied] = @RowsCopied,
            p.[ErrorCode] = @NormalizedErrorCode,
            p.[ErrorMessage] = @NormalizedErrorMessage,
            p.[UpdatedUtc] = @NowUtc
        FROM
            [monitor].[PipelineRunLog] AS p
        WHERE
            p.[PipelineRunId] = @NormalizedPipelineRunId;

        IF @@ROWCOUNT = 0
        BEGIN
            THROW 51001, 'PipelineRunId not found in monitor.PipelineRunLog.', 1;
        END;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH;
END;
GO
