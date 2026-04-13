-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-14
-- Name: snapshot_scd2.usp_DetectDeltaAndRefreshState
-- Description: Builds today comparable rows, detects delta by Rowhash, refreshes comparable state.
-- Version: 1.0
-- ----------------------------------------------------------------------

CREATE PROCEDURE [snapshot_scd2].[usp_DetectDeltaAndRefreshState]
    @PipelineRunId NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @ProcStartUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @ProcEndUtc DATETIME2(3) = NULL;
    DECLARE @ProcObjectId INT = OBJECT_ID(N'[snapshot_scd2].[usp_DetectDeltaAndRefreshState]');

    DECLARE @RowsRead BIGINT = 0;
    DECLARE @RowsScanned BIGINT = 0;
    DECLARE @RowsWritten BIGINT = 0;
    DECLARE @RowsInserted BIGINT = 0;
    DECLARE @RowsUpdated BIGINT = 0;
    DECLARE @RowsExpired BIGINT = 0;
    DECLARE @DeltaCount BIGINT = 0;

    DECLARE @CpuDelta BIGINT = NULL;
    DECLARE @LogicalReadsDelta BIGINT = NULL;
    DECLARE @PhysicalReadsDelta BIGINT = NULL;
    DECLARE @WritesDelta BIGINT = NULL;

    DECLARE @ErrorNumber INT = NULL;
    DECLARE @ErrorMessage NVARCHAR(4000) = NULL;

    IF OBJECT_ID(N'[snapshot_scd2].[raw_Users]', N'U') IS NULL
    BEGIN
        THROW 52001, 'Required table [snapshot_scd2].[raw_Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[snapshot_scd2].[UserComparableState]', N'U') IS NULL
    BEGIN
        THROW 52002, 'Required table [snapshot_scd2].[UserComparableState] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[snapshot_scd2].[TodayComparable]', N'U') IS NULL
    BEGIN
        THROW 52003, 'Required table [snapshot_scd2].[TodayComparable] does not exist.', 1;
    END;

    IF @NormalizedPipelineRunId IS NOT NULL
    BEGIN
        EXEC [monitor].[usp_ProcedureRunStart]
            @PipelineRunId = @NormalizedPipelineRunId,
            @ProcedureName = N'snapshot_scd2.usp_DetectDeltaAndRefreshState',
            @ProcedurePhase = N'Other';
    END;

    BEGIN TRY
        BEGIN TRAN;

        TRUNCATE TABLE [snapshot_scd2].[TodayComparable];

        SELECT
            r.[Email],
            r.[Username],
            r.[SubscriptionTier],
            r.[BillingCycle],
            r.[PaymentMethod],
            r.[AutoRenew],
            r.[MarketingConsent],
            r.[PreferredLanguage],
            r.[ContentLanguage],
            r.[PlanAddons],
            CONCAT(
                ISNULL(r.[Email], ''), '|',
                ISNULL(r.[Username], ''), '|',
                ISNULL(r.[SubscriptionTier], ''), '|',
                ISNULL(r.[BillingCycle], ''), '|',
                ISNULL(r.[PaymentMethod], ''), '|',
                ISNULL(r.[AutoRenew], ''), '|',
                ISNULL(r.[MarketingConsent], ''), '|',
                ISNULL(r.[PreferredLanguage], ''), '|',
                ISNULL(r.[ContentLanguage], ''), '|',
                ISNULL(r.[PlanAddons], '')
            ) AS [Hashdata],
            HASHBYTES(
                'SHA2_256',
                CONCAT(
                    ISNULL(r.[Email], ''), '|',
                    ISNULL(r.[Username], ''), '|',
                    ISNULL(r.[SubscriptionTier], ''), '|',
                    ISNULL(r.[BillingCycle], ''), '|',
                    ISNULL(r.[PaymentMethod], ''), '|',
                    ISNULL(r.[AutoRenew], ''), '|',
                    ISNULL(r.[MarketingConsent], ''), '|',
                    ISNULL(r.[PreferredLanguage], ''), '|',
                    ISNULL(r.[ContentLanguage], ''), '|',
                    ISNULL(r.[PlanAddons], '')
                )
            ) AS [Rowhash]
        INTO #CurrentComparable
        FROM [snapshot_scd2].[raw_Users] AS r;

        CREATE NONCLUSTERED INDEX [IX__CurrentComparable_Rowhash]
            ON #CurrentComparable ([Rowhash]);

        INSERT INTO [snapshot_scd2].[TodayComparable]
        (
            [Email],
            [Username],
            [SubscriptionTier],
            [BillingCycle],
            [PaymentMethod],
            [AutoRenew],
            [MarketingConsent],
            [PreferredLanguage],
            [ContentLanguage],
            [PlanAddons],
            [Hashdata],
            [Rowhash],
            [LastRefreshedDate]
        )
        SELECT
            c.[Email],
            c.[Username],
            c.[SubscriptionTier],
            c.[BillingCycle],
            c.[PaymentMethod],
            c.[AutoRenew],
            c.[MarketingConsent],
            c.[PreferredLanguage],
            c.[ContentLanguage],
            c.[PlanAddons],
            c.[Hashdata],
            c.[Rowhash],
            SYSUTCDATETIME()
        FROM #CurrentComparable AS c
        LEFT JOIN [snapshot_scd2].[UserComparableState] AS s
            ON ISNULL(s.[Rowhash], 0x0) = ISNULL(c.[Rowhash], 0x0)
        WHERE s.[Id] IS NULL;

        SET @DeltaCount = @@ROWCOUNT;

        DELETE s
        FROM [snapshot_scd2].[UserComparableState] AS s;

        INSERT INTO [snapshot_scd2].[UserComparableState]
        (
            [Email],
            [Username],
            [SubscriptionTier],
            [BillingCycle],
            [PaymentMethod],
            [AutoRenew],
            [MarketingConsent],
            [PreferredLanguage],
            [ContentLanguage],
            [PlanAddons],
            [Hashdata],
            [Rowhash],
            [LastRefreshedDate]
        )
        SELECT
            c.[Email],
            c.[Username],
            c.[SubscriptionTier],
            c.[BillingCycle],
            c.[PaymentMethod],
            c.[AutoRenew],
            c.[MarketingConsent],
            c.[PreferredLanguage],
            c.[ContentLanguage],
            c.[PlanAddons],
            c.[Hashdata],
            c.[Rowhash],
            SYSUTCDATETIME()
        FROM #CurrentComparable AS c;

        SET @RowsRead = (SELECT COUNT_BIG(1) FROM [snapshot_scd2].[raw_Users]);
        SET @RowsInserted = @DeltaCount;
        SET @RowsWritten = @DeltaCount;
        SET @RowsScanned = @RowsRead;

        COMMIT TRAN;

        SET @ProcEndUtc = SYSUTCDATETIME();

        IF EXISTS (
            SELECT 1
            FROM [sys].[database_query_store_options] AS qo
            WHERE qo.[actual_state_desc] IN ('READ_WRITE', 'READ_ONLY')
        )
        BEGIN
            BEGIN TRY
                SELECT
                    @CpuDelta = CAST(SUM(CAST(rs.[avg_cpu_time] AS DECIMAL(38, 6)) * rs.[count_executions]) / 1000.0 AS BIGINT),
                    @LogicalReadsDelta = CAST(SUM(CAST(rs.[avg_logical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
                    @PhysicalReadsDelta = CAST(SUM(CAST(rs.[avg_physical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
                    @WritesDelta = CAST(SUM(CAST(rs.[avg_logical_io_writes] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT)
                FROM
                    [sys].[query_store_query] AS q
                INNER JOIN [sys].[query_store_plan] AS p
                    ON q.[query_id] = p.[query_id]
                INNER JOIN [sys].[query_store_runtime_stats] AS rs
                    ON p.[plan_id] = rs.[plan_id]
                INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
                    ON rs.[runtime_stats_interval_id] = rsi.[runtime_stats_interval_id]
                WHERE
                    q.[object_id] = @ProcObjectId
                    AND rsi.[start_time] < @ProcEndUtc
                    AND rsi.[end_time] > @ProcStartUtc;
            END TRY
            BEGIN CATCH
                SET @CpuDelta = NULL;
                SET @LogicalReadsDelta = NULL;
                SET @PhysicalReadsDelta = NULL;
                SET @WritesDelta = NULL;
            END CATCH;
        END;

        IF @NormalizedPipelineRunId IS NOT NULL
        BEGIN
            EXEC [monitor].[usp_ProcedureRunFinish]
                @PipelineRunId = @NormalizedPipelineRunId,
                @ProcedureName = N'snapshot_scd2.usp_DetectDeltaAndRefreshState',
                @Status = N'Succeeded',
                @RowsRead = @RowsRead,
                @RowsScanned = @RowsScanned,
                @RowsWritten = @RowsWritten,
                @RowsInserted = @RowsInserted,
                @RowsUpdated = @RowsUpdated,
                @RowsExpired = @RowsExpired,
                @CpuTimeMs = @CpuDelta,
                @LogicalReads = @LogicalReadsDelta,
                @PhysicalReads = @PhysicalReadsDelta,
                @Writes = @WritesDelta;
        END;
    END TRY
    BEGIN CATCH
        SET @ErrorNumber = ERROR_NUMBER();
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRAN;
        END;

        SET @ProcEndUtc = SYSUTCDATETIME();

        IF EXISTS (
            SELECT 1
            FROM [sys].[database_query_store_options] AS qo
            WHERE qo.[actual_state_desc] IN ('READ_WRITE', 'READ_ONLY')
        )
        BEGIN
            BEGIN TRY
                SELECT
                    @CpuDelta = CAST(SUM(CAST(rs.[avg_cpu_time] AS DECIMAL(38, 6)) * rs.[count_executions]) / 1000.0 AS BIGINT),
                    @LogicalReadsDelta = CAST(SUM(CAST(rs.[avg_logical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
                    @PhysicalReadsDelta = CAST(SUM(CAST(rs.[avg_physical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
                    @WritesDelta = CAST(SUM(CAST(rs.[avg_logical_io_writes] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT)
                FROM
                    [sys].[query_store_query] AS q
                INNER JOIN [sys].[query_store_plan] AS p
                    ON q.[query_id] = p.[query_id]
                INNER JOIN [sys].[query_store_runtime_stats] AS rs
                    ON p.[plan_id] = rs.[plan_id]
                INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
                    ON rs.[runtime_stats_interval_id] = rsi.[runtime_stats_interval_id]
                WHERE
                    q.[object_id] = @ProcObjectId
                    AND rsi.[start_time] < @ProcEndUtc
                    AND rsi.[end_time] > @ProcStartUtc;
            END TRY
            BEGIN CATCH
                SET @CpuDelta = NULL;
                SET @LogicalReadsDelta = NULL;
                SET @PhysicalReadsDelta = NULL;
                SET @WritesDelta = NULL;
            END CATCH;
        END;

        IF @NormalizedPipelineRunId IS NOT NULL
        BEGIN
            BEGIN TRY
                EXEC [monitor].[usp_ProcedureRunFinish]
                    @PipelineRunId = @NormalizedPipelineRunId,
                    @ProcedureName = N'snapshot_scd2.usp_DetectDeltaAndRefreshState',
                    @Status = N'Failed',
                    @RowsRead = @RowsRead,
                    @RowsScanned = @RowsScanned,
                    @RowsWritten = @RowsWritten,
                    @RowsInserted = @RowsInserted,
                    @RowsUpdated = @RowsUpdated,
                    @RowsExpired = @RowsExpired,
                    @CpuTimeMs = @CpuDelta,
                    @LogicalReads = @LogicalReadsDelta,
                    @PhysicalReads = @PhysicalReadsDelta,
                    @Writes = @WritesDelta,
                    @ErrorNumber = @ErrorNumber,
                    @ErrorMessage = @ErrorMessage;
            END TRY
            BEGIN CATCH
            END CATCH;
        END;

        THROW;
    END CATCH;
END;
GO
