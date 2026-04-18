-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-18
-- Name: snapshot_scd2.usp_ProcessUsers
-- Description: Snapshot SCD2 staging procedure.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [snapshot_scd2].[usp_ProcessUsers]
    @PipelineRunId NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @RowsRead BIGINT = 0;
    DECLARE @RowsScanned BIGINT = 0;
    DECLARE @RowsWritten BIGINT = 0;
    DECLARE @RowsInserted BIGINT = 0;
    DECLARE @RowsUpdated BIGINT = 0;
    DECLARE @RowsExpired BIGINT = 0;

    DECLARE @ProcStartUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @ProcEndUtc DATETIME2(3) = NULL;
    DECLARE @ProcObjectId INT = OBJECT_ID(N'[snapshot_scd2].[usp_ProcessUsers]');

    DECLARE @CpuDelta BIGINT = NULL;
    DECLARE @LogicalReadsDelta BIGINT = NULL;
    DECLARE @PhysicalReadsDelta BIGINT = NULL;
    DECLARE @WritesDelta BIGINT = NULL;

    DECLARE @ErrorNumber INT = NULL;
    DECLARE @ErrorMessage NVARCHAR(4000) = NULL;

    IF OBJECT_ID(N'[snapshot_scd2].[stage_Users]', N'U') IS NULL
    BEGIN
        THROW 54001, 'Required table [snapshot_scd2].[stage_Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[snapshot_scd2].[raw_Users]', N'U') IS NULL
    BEGIN
        THROW 54002, 'Required table [snapshot_scd2].[raw_Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[snapshot_scd2].[UserComparableState]', N'U') IS NULL
    BEGIN
        THROW 54004, 'Required table [snapshot_scd2].[UserComparableState] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[config].[Countries]', N'U') IS NULL
    BEGIN
        THROW 54005, 'Required table [config].[Countries] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[config].[SubscriptionTiers]', N'U') IS NULL
    BEGIN
        THROW 54006, 'Required table [config].[SubscriptionTiers] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[config].[PaymentMethods]', N'U') IS NULL
    BEGIN
        THROW 54007, 'Required table [config].[PaymentMethods] does not exist.', 1;
    END;

    IF @NormalizedPipelineRunId IS NOT NULL
    BEGIN
        EXEC [monitor].[usp_ProcedureRunStart]
            @PipelineRunId = @NormalizedPipelineRunId,
            @ProcedureName = N'snapshot_scd2.usp_ProcessUsers',
            @ProcedurePhase = N'Stage';
    END;

    BEGIN TRY
        BEGIN TRAN;

        TRUNCATE TABLE [snapshot_scd2].[stage_Users];

        IF OBJECT_ID('tempdb..#CurrentComparable') IS NOT NULL
        BEGIN
            DROP TABLE #CurrentComparable;
        END;

        CREATE TABLE #CurrentComparable
        (
            [Email] NVARCHAR(320) NULL,
            [Username] NVARCHAR(120) NULL,
            [SubscriptionTier] NVARCHAR(40) NULL,
            [BillingCycle] NVARCHAR(40) NULL,
            [PaymentMethod] NVARCHAR(40) NULL,
            [AutoRenew] NVARCHAR(3) NULL,
            [MarketingConsent] NVARCHAR(3) NULL,
            [PreferredLanguage] NVARCHAR(10) NULL,
            [ContentLanguage] NVARCHAR(10) NULL,
            [PlanAddons] NVARCHAR(100) NULL,
            [Hashdata] VARCHAR(MAX) NULL,
            [Rowhash] VARBINARY(6000) NULL,
            [LastRefreshedDate] DATETIME NULL
        );

        CREATE NONCLUSTERED INDEX [IX_CurrentComparable_Email_Username]
            ON #CurrentComparable ([Email], [Username])
            INCLUDE ([Rowhash]);

        INSERT INTO #CurrentComparable
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
            LOWER(TRIM(r.[Email])),
            LOWER(TRIM(r.[Username])),
            TRIM(r.[SubscriptionTier]),
            TRIM(r.[BillingCycle]),
            TRIM(r.[PaymentMethod]),
            CASE WHEN r.[AutoRenew] = '1' THEN 'Yes' ELSE 'No' END,
            CASE WHEN r.[MarketingConsent] = '1' THEN 'Yes' ELSE 'No' END,
            LOWER(TRIM(r.[PreferredLanguage])),
            LOWER(TRIM(r.[ContentLanguage])),
            TRIM(r.[PlanAddons]),
            CONCAT(
                ISNULL(LOWER(TRIM(r.[Email])), ''), '|',
                ISNULL(LOWER(TRIM(r.[Username])), ''), '|',
                ISNULL(TRIM(r.[SubscriptionTier]), ''), '|',
                ISNULL(TRIM(r.[BillingCycle]), ''), '|',
                ISNULL(TRIM(r.[PaymentMethod]), ''), '|',
                ISNULL(CASE WHEN r.[AutoRenew] = '1' THEN 'Yes' ELSE 'No' END, ''), '|',
                ISNULL(CASE WHEN r.[MarketingConsent] = '1' THEN 'Yes' ELSE 'No' END, ''), '|',
                ISNULL(LOWER(TRIM(r.[PreferredLanguage])), ''), '|',
                ISNULL(LOWER(TRIM(r.[ContentLanguage])), ''), '|',
                ISNULL(TRIM(r.[PlanAddons]), '')
            ),
            HASHBYTES('SHA2_256', CONCAT(
                ISNULL(LOWER(TRIM(r.[Email])), ''), '|',
                ISNULL(LOWER(TRIM(r.[Username])), ''), '|',
                ISNULL(TRIM(r.[SubscriptionTier]), ''), '|',
                ISNULL(TRIM(r.[BillingCycle]), ''), '|',
                ISNULL(TRIM(r.[PaymentMethod]), ''), '|',
                ISNULL(CASE WHEN r.[AutoRenew] = '1' THEN 'Yes' ELSE 'No' END, ''), '|',
                ISNULL(CASE WHEN r.[MarketingConsent] = '1' THEN 'Yes' ELSE 'No' END, ''), '|',
                ISNULL(LOWER(TRIM(r.[PreferredLanguage])), ''), '|',
                ISNULL(LOWER(TRIM(r.[ContentLanguage])), ''), '|',
                ISNULL(TRIM(r.[PlanAddons]), '')
            )),
            CURRENT_TIMESTAMP
        FROM [snapshot_scd2].[raw_Users] AS r;

        IF OBJECT_ID('tempdb..#TodayComparableUsers') IS NOT NULL
        BEGIN
            DROP TABLE #TodayComparableUsers;
        END;

        CREATE TABLE #TodayComparableUsers
        (
            [Email] NVARCHAR(320) NULL,
            [Username] NVARCHAR(120) NULL,
            [SubscriptionTier] NVARCHAR(40) NULL,
            [BillingCycle] NVARCHAR(40) NULL,
            [PaymentMethod] NVARCHAR(40) NULL,
            [AutoRenew] NVARCHAR(3) NULL,
            [MarketingConsent] NVARCHAR(3) NULL,
            [PreferredLanguage] NVARCHAR(10) NULL,
            [ContentLanguage] NVARCHAR(10) NULL,
            [PlanAddons] NVARCHAR(100) NULL,
            [Hashdata] VARCHAR(MAX) NULL,
            [Rowhash] VARBINARY(6000) NULL,
            [LastRefreshedDate] DATETIME NULL
        );

        CREATE NONCLUSTERED INDEX [IX_TodayComparableUsers_Email_Username]
            ON #TodayComparableUsers ([Email], [Username])
            INCLUDE ([Rowhash]);

        INSERT INTO #TodayComparableUsers
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
            c.[LastRefreshedDate]
        FROM #CurrentComparable AS c
        LEFT JOIN [snapshot_scd2].[UserComparableState] AS s
            ON ISNULL(s.[Email], '') = ISNULL(c.[Email], '')
            AND ISNULL(s.[Username], '') = ISNULL(c.[Username], '')
        WHERE s.[Id] IS NULL
            OR ISNULL(s.[Rowhash], 0x0) <> ISNULL(c.[Rowhash], 0x0);

        SET @RowsRead = (SELECT COUNT_BIG(*) FROM [snapshot_scd2].[raw_Users]);
        INSERT INTO [snapshot_scd2].[stage_Users]
        (
            [FirstName],
            [LastName],
            [Email],
            [Username],
            [DateOfBirth],
            [RegistrationDate],
            [Country],
            [City],
            [Gender],
            [AccountCreatedVia],
            [ReferralSource],
            [SubscriptionTier],
            [BillingCycle],
            [PaymentMethod],
            [AutoRenew],
            [MarketingConsent],
            [PreferredLanguage],
            [ContentLanguage],
            [PlanAddons],
            [LastRefreshedDate]
        )
        SELECT
            r.[FirstName],
            r.[LastName],
            r.[Email],
            r.[Username],
            r.[DateOfBirth],
            r.[RegistrationDate],
            r.[Country],
            r.[City],
            r.[Gender],
            r.[AccountCreatedVia],
            r.[ReferralSource],
            r.[SubscriptionTier],
            r.[BillingCycle],
            r.[PaymentMethod],
            CASE WHEN r.[AutoRenew] = '1' THEN 'Yes' ELSE 'No' END,
            CASE WHEN r.[MarketingConsent] = '1' THEN 'Yes' ELSE 'No' END,
            r.[PreferredLanguage],
            r.[ContentLanguage],
            r.[PlanAddons],
            CURRENT_TIMESTAMP
        FROM [snapshot_scd2].[raw_Users] AS r
        INNER JOIN #TodayComparableUsers AS t
            ON ISNULL(LOWER(TRIM(r.[Email])), '') = ISNULL(t.[Email], '')
            AND ISNULL(LOWER(TRIM(r.[Username])), '') = ISNULL(t.[Username], '');

        SET @RowsInserted = @@ROWCOUNT;
        SET @RowsWritten = @RowsInserted;
        SET @RowsScanned = @RowsRead;

        UPDATE s
        SET
            s.[FirstName] = TRIM(s.[FirstName]),
            s.[LastName] = TRIM(s.[LastName]),
            s.[Email] = LOWER(TRIM(s.[Email])),
            s.[Username] = LOWER(TRIM(s.[Username])),
            s.[Country] = TRIM(s.[Country]),
            s.[City] = TRIM(s.[City]),
            s.[Gender] = TRIM(s.[Gender]),
            s.[AccountCreatedVia] = TRIM(s.[AccountCreatedVia]),
            s.[ReferralSource] = TRIM(s.[ReferralSource]),
            s.[SubscriptionTier] = TRIM(s.[SubscriptionTier]),
            s.[BillingCycle] = TRIM(s.[BillingCycle]),
            s.[PaymentMethod] = TRIM(s.[PaymentMethod]),
            s.[PreferredLanguage] = LOWER(TRIM(s.[PreferredLanguage])),
            s.[ContentLanguage] = LOWER(TRIM(s.[ContentLanguage])),
            s.[PlanAddons] = TRIM(s.[PlanAddons])
        FROM [snapshot_scd2].[stage_Users] AS s;

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[FullName] = CONCAT(ISNULL(s.[FirstName], ''), ' ', ISNULL(s.[LastName], '')),
            s.[YearOfBirth] = CASE WHEN s.[DateOfBirth] IS NULL THEN NULL ELSE CAST(YEAR(s.[DateOfBirth]) AS VARCHAR(4)) END,
            s.[MonthOfBirth] = CASE WHEN s.[DateOfBirth] IS NULL THEN NULL ELSE DATENAME(MONTH, s.[DateOfBirth]) END,
            s.[DayOfBirth] = CASE WHEN s.[DateOfBirth] IS NULL THEN NULL ELSE RIGHT(CONCAT('0', CAST(DAY(s.[DateOfBirth]) AS VARCHAR(2))), 2) END,
            s.[LastRefreshedDate] = CURRENT_TIMESTAMP
        FROM [snapshot_scd2].[stage_Users] AS s;

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET s.[CountryCode] = c.[CountryCode]
        FROM [snapshot_scd2].[stage_Users] AS s
        LEFT JOIN [config].[Countries] AS c
            ON s.[Country] = c.[CountryName];

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[SubscriptionTierRank] = t.[TierRank],
            s.[IsPaidTier] = CASE WHEN t.[IsPaid] = 1 THEN 'Yes' WHEN t.[IsPaid] = 0 THEN 'No' END
        FROM [snapshot_scd2].[stage_Users] AS s
        LEFT JOIN [config].[SubscriptionTiers] AS t
            ON s.[SubscriptionTier] = t.[TierCode];

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[PaymentMethodGroup] = p.[PaymentMethodGroup],
            s.[IsCardBased] = CASE WHEN p.[IsCardBased] = 1 THEN 'Yes' WHEN p.[IsCardBased] = 0 THEN 'No' END
        FROM [snapshot_scd2].[stage_Users] AS s
        LEFT JOIN [config].[PaymentMethods] AS p
            ON s.[PaymentMethod] = p.[PaymentMethodCode];

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[Hashdata] = CONCAT(
                ISNULL(s.[Email], ''), '|',
                ISNULL(s.[Username], ''), '|',
                ISNULL(s.[SubscriptionTier], ''), '|',
                ISNULL(s.[BillingCycle], ''), '|',
                ISNULL(s.[PaymentMethod], ''), '|',
                ISNULL(s.[AutoRenew], ''), '|',
                ISNULL(s.[MarketingConsent], ''), '|',
                ISNULL(s.[PreferredLanguage], ''), '|',
                ISNULL(s.[ContentLanguage], ''), '|',
                ISNULL(s.[PlanAddons], '')
            ),
            s.[Rowhash] = HASHBYTES('SHA2_256', CONCAT(
                ISNULL(s.[Email], ''), '|',
                ISNULL(s.[Username], ''), '|',
                ISNULL(s.[SubscriptionTier], ''), '|',
                ISNULL(s.[BillingCycle], ''), '|',
                ISNULL(s.[PaymentMethod], ''), '|',
                ISNULL(s.[AutoRenew], ''), '|',
                ISNULL(s.[MarketingConsent], ''), '|',
                ISNULL(s.[PreferredLanguage], ''), '|',
                ISNULL(s.[ContentLanguage], ''), '|',
                ISNULL(s.[PlanAddons], '')
            ))
        FROM [snapshot_scd2].[stage_Users] AS s;

        SET @RowsScanned = @RowsRead + @RowsUpdated;

        TRUNCATE TABLE [snapshot_scd2].[UserComparableState];

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
            c.[LastRefreshedDate]
        FROM #CurrentComparable AS c;

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
                FROM [sys].[query_store_query] AS q
                INNER JOIN [sys].[query_store_plan] AS p
                    ON q.[query_id] = p.[query_id]
                INNER JOIN [sys].[query_store_runtime_stats] AS rs
                    ON p.[plan_id] = rs.[plan_id]
                INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
                    ON rs.[runtime_stats_interval_id] = rsi.[runtime_stats_interval_id]
                WHERE q.[object_id] = @ProcObjectId
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
                @ProcedureName = N'snapshot_scd2.usp_ProcessUsers',
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
                FROM [sys].[query_store_query] AS q
                INNER JOIN [sys].[query_store_plan] AS p
                    ON q.[query_id] = p.[query_id]
                INNER JOIN [sys].[query_store_runtime_stats] AS rs
                    ON p.[plan_id] = rs.[plan_id]
                INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
                    ON rs.[runtime_stats_interval_id] = rsi.[runtime_stats_interval_id]
                WHERE q.[object_id] = @ProcObjectId
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
                    @ProcedureName = N'snapshot_scd2.usp_ProcessUsers',
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
