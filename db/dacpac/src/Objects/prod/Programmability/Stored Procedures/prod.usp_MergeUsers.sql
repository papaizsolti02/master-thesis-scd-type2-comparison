-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: prod.usp_MergeUsers
-- Description: SCD2 merge from stage.Users to prod.Users using update + insert.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [prod].[usp_MergeUsers]
    @PipelineRunId NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @AsOfDate DATETIME = CURRENT_TIMESTAMP;
    DECLARE @MatchedCount INT = 0;
    DECLARE @ExpiredCount INT = 0;
    DECLARE @InsertedCount INT = 0;

    DECLARE @RowsRead BIGINT = 0;
    DECLARE @RowsScanned BIGINT = 0;
    DECLARE @RowsWritten BIGINT = 0;
    DECLARE @RowsInserted BIGINT = 0;
    DECLARE @RowsUpdated BIGINT = 0;
    DECLARE @RowsExpired BIGINT = 0;

    DECLARE @ProcStartUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @ProcEndUtc DATETIME2(3) = NULL;
    DECLARE @ProcObjectId INT = OBJECT_ID(N'[prod].[usp_MergeUsers]');

    DECLARE @CpuDelta BIGINT = NULL;
    DECLARE @LogicalReadsDelta BIGINT = NULL;
    DECLARE @PhysicalReadsDelta BIGINT = NULL;
    DECLARE @WritesDelta BIGINT = NULL;

    DECLARE @ErrorNumber INT = NULL;
    DECLARE @ErrorMessage NVARCHAR(4000) = NULL;

    IF OBJECT_ID(N'[stage].[Users]', N'U') IS NULL
    BEGIN
        THROW 50101, 'Required table [stage].[Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[prod].[Users]', N'U') IS NULL
    BEGIN
        THROW 50102, 'Required table [prod].[Users] does not exist.', 1;
    END;

    IF @NormalizedPipelineRunId IS NOT NULL
    BEGIN
        EXEC [monitor].[usp_ProcedureRunStart]
            @PipelineRunId = @NormalizedPipelineRunId,
            @ProcedureName = N'prod.usp_MergeUsers',
            @ProcedurePhase = N'Merge';
    END;

    BEGIN TRY
        BEGIN TRAN;

        SELECT @RowsRead = COUNT_BIG(1)
        FROM [stage].[Users];

        -- 1) Update current rows where hash is present in stage (update non hash fields)
        UPDATE p
        SET
            p.[FullName] = s.[FullName],
            p.[FirstName] = s.[FirstName],
            p.[LastName] = s.[LastName],
            p.[DateOfBirth] = s.[DateOfBirth],
            p.[YearOfBirth] = s.[YearOfBirth],
            p.[MonthOfBirth] = s.[MonthOfBirth],
            p.[DayOfBirth] = s.[DayOfBirth],
            p.[RegistrationDate] = s.[RegistrationDate],
            p.[Country] = s.[Country],
            p.[CountryCode] = s.[CountryCode],
            p.[City] = s.[City],
            p.[Gender] = s.[Gender],
            p.[AccountCreatedVia] = s.[AccountCreatedVia],
            p.[ReferralSource] = s.[ReferralSource],
            p.[SubscriptionTier] = s.[SubscriptionTier],
            p.[SubscriptionTierRank] = s.[SubscriptionTierRank],
            p.[IsPaidTier] = s.[IsPaidTier],
            p.[BillingCycle] = s.[BillingCycle],
            p.[PaymentMethod] = s.[PaymentMethod],
            p.[PaymentMethodGroup] = s.[PaymentMethodGroup],
            p.[IsCardBased] = s.[IsCardBased],
            p.[AutoRenew] = s.[AutoRenew],
            p.[MarketingConsent] = s.[MarketingConsent],
            p.[PreferredLanguage] = s.[PreferredLanguage],
            p.[ContentLanguage] = s.[ContentLanguage],
            p.[PlanAddons] = s.[PlanAddons]
        FROM
            [prod].[Users] AS p
        INNER JOIN [stage].[Users] AS s
            ON ISNULL(p.[Rowhash], 0x0) = ISNULL(s.[Rowhash], 0x0)
        WHERE
            p.[IsActive] = 1;

        SET @MatchedCount = @@ROWCOUNT;

        -- 2) Expire active rows whose hash is no longer present in stage
        UPDATE p
        SET
            p.[IsActive] = 0,
            p.[ExpirationDate] = @AsOfDate,
            p.[LastRefreshedDate] = @AsOfDate
        FROM
            [prod].[Users] AS p
        WHERE
            p.[IsActive] = 1
            AND NOT EXISTS
            (
                SELECT 1
                FROM [stage].[Users] AS s
                WHERE ISNULL(s.[Rowhash], 0x0) = ISNULL(p.[Rowhash], 0x0)
            );

        SET @ExpiredCount = @@ROWCOUNT;

        -- 3) Insert rows whose hash cannot be found in current active prod
        INSERT INTO [prod].[Users]
        (
            [FullName],
            [FirstName],
            [LastName],
            [Email],
            [Username],
            [DateOfBirth],
            [YearOfBirth],
            [MonthOfBirth],
            [DayOfBirth],
            [RegistrationDate],
            [Country],
            [CountryCode],
            [City],
            [Gender],
            [AccountCreatedVia],
            [ReferralSource],
            [SubscriptionTier],
            [SubscriptionTierRank],
            [IsPaidTier],
            [BillingCycle],
            [PaymentMethod],
            [PaymentMethodGroup],
            [IsCardBased],
            [AutoRenew],
            [MarketingConsent],
            [PreferredLanguage],
            [ContentLanguage],
            [PlanAddons],
            [LastRefreshedDate],
            [EffectiveDate],
            [ExpirationDate],
            [IsActive],
            [Hashdata],
            [Rowhash]
        )
        SELECT
            s.[FullName],
            s.[FirstName],
            s.[LastName],
            s.[Email],
            s.[Username],
            s.[DateOfBirth],
            s.[YearOfBirth],
            s.[MonthOfBirth],
            s.[DayOfBirth],
            s.[RegistrationDate],
            s.[Country],
            s.[CountryCode],
            s.[City],
            s.[Gender],
            s.[AccountCreatedVia],
            s.[ReferralSource],
            s.[SubscriptionTier],
            s.[SubscriptionTierRank],
            s.[IsPaidTier],
            s.[BillingCycle],
            s.[PaymentMethod],
            s.[PaymentMethodGroup],
            s.[IsCardBased],
            s.[AutoRenew],
            s.[MarketingConsent],
            s.[PreferredLanguage],
            s.[ContentLanguage],
            s.[PlanAddons],
            @AsOfDate AS [LastRefreshedDate],
            @AsOfDate AS [EffectiveDate],
            '9999-12-31 00:00:00' AS [ExpirationDate],
            1 AS [IsActive],
            s.[Hashdata],
            s.[Rowhash]
        FROM
            [stage].[Users] AS s
        LEFT JOIN [prod].[Users] AS p
            ON ISNULL(p.[Rowhash], 0x0) = ISNULL(s.[Rowhash], 0x0)
            AND p.[IsActive] = 1
        WHERE
            p.[Id] IS NULL;

        SET @InsertedCount = @@ROWCOUNT;

        SET @RowsInserted = @InsertedCount;
        SET @RowsWritten = @InsertedCount;
        SET @RowsExpired = @ExpiredCount;

        UPDATE p
        SET
            p.[LastRefreshedDate] = @AsOfDate
        FROM
            [prod].[Users] AS p;

        SET @RowsUpdated = CAST(@MatchedCount AS BIGINT);
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
                    ON p.[query_id] = q.[query_id]
                INNER JOIN [sys].[query_store_runtime_stats] AS rs
                    ON rs.[plan_id] = p.[plan_id]
                INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
                    ON rsi.[runtime_stats_interval_id] = rs.[runtime_stats_interval_id]
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
                @ProcedureName = N'prod.usp_MergeUsers',
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

        SELECT
            @AsOfDate AS [MergeTimestamp],
            @MatchedCount AS [MatchedRows],
            @ExpiredCount AS [ExpiredRows],
            @InsertedCount AS [InsertedRows];
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

        IF @RowsScanned = 0
        BEGIN
            SET @RowsScanned = @RowsRead;
        END;

        IF @NormalizedPipelineRunId IS NOT NULL
        BEGIN
            BEGIN TRY
                EXEC [monitor].[usp_ProcedureRunFinish]
                    @PipelineRunId = @NormalizedPipelineRunId,
                    @ProcedureName = N'prod.usp_MergeUsers',
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
                -- Preserve original ETL failure if monitoring logging fails.
            END CATCH;
        END;

        THROW;
    END CATCH;
END;
GO
