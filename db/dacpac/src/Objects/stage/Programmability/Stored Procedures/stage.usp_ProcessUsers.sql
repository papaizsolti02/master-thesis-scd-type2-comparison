-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: stage.usp_ProcessUsers
-- Description: Truncates stage.Users, loads from raw.Users, and processes data.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [stage].[usp_ProcessUsers] -- noqa:
    @PipelineRunId NVARCHAR(128) = NULL,
    @SCD2Method NVARCHAR(50) = N'FullSCD2'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
    DECLARE @NormalizedMethod NVARCHAR(50) = UPPER(ISNULL(NULLIF(TRIM(@SCD2Method), ''), 'FULLSCD2'));
    DECLARE @TargetSchema SYSNAME = NULL;
    DECLARE @RawUsersTable NVARCHAR(300) = NULL;
    DECLARE @StageUsersTable NVARCHAR(300) = NULL;
    DECLARE @Sql NVARCHAR(MAX) = NULL;
    DECLARE @RowsTouched BIGINT = 0;
    DECLARE @RowsRead BIGINT = 0;
    DECLARE @RowsScanned BIGINT = 0;
    DECLARE @RowsWritten BIGINT = 0;
    DECLARE @RowsInserted BIGINT = 0;
    DECLARE @RowsUpdated BIGINT = 0;
    DECLARE @RowsExpired BIGINT = 0;

    DECLARE @ProcStartUtc DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @ProcEndUtc DATETIME2(3) = NULL;
    DECLARE @ProcObjectId INT = OBJECT_ID(N'[stage].[usp_ProcessUsers]');

    DECLARE @CpuDelta BIGINT = NULL;
    DECLARE @LogicalReadsDelta BIGINT = NULL;
    DECLARE @PhysicalReadsDelta BIGINT = NULL;
    DECLARE @WritesDelta BIGINT = NULL;

    DECLARE @ErrorNumber INT = NULL;
    DECLARE @ErrorMessage NVARCHAR(4000) = NULL;

    IF @NormalizedMethod IN (N'FULLSCD2', N'FULL')
    BEGIN
        SET @TargetSchema = N'full_scd2';
    END
    ELSE IF @NormalizedMethod IN (N'SNAPSHOTSCD2', N'SNAPSHOT')
    BEGIN
        SET @TargetSchema = N'snapshot_scd2';
    END
    ELSE IF @NormalizedMethod IN (N'MERKLESCD2', N'MERKLE')
    BEGIN
        SET @TargetSchema = N'merkle_scd2';
    END
    ELSE
    BEGIN
        THROW 50006, 'Unsupported SCD2Method. Allowed: FullSCD2, SnapshotSCD2, MerkleSCD2.', 1;
    END;

    SET @RawUsersTable = QUOTENAME(@TargetSchema) + N'.[raw_Users]';
    SET @StageUsersTable = QUOTENAME(@TargetSchema) + N'.[stage_Users]';

    IF OBJECT_ID(@StageUsersTable, N'U') IS NULL
    BEGIN
        THROW 50001, 'Required stage table for selected method does not exist.', 1;
    END;

    IF OBJECT_ID(@RawUsersTable, N'U') IS NULL
    BEGIN
        THROW 50002, 'Required raw table for selected method does not exist.', 1;
    END;

    IF OBJECT_ID(N'[config].[Countries]', N'U') IS NULL
    BEGIN
        THROW 50003, 'Required table [config].[Countries] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[config].[SubscriptionTiers]', N'U') IS NULL
    BEGIN
        THROW 50004, 'Required table [config].[SubscriptionTiers] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[config].[PaymentMethods]', N'U') IS NULL
    BEGIN
        THROW 50005, 'Required table [config].[PaymentMethods] does not exist.', 1;
    END;

    IF @NormalizedPipelineRunId IS NOT NULL
    BEGIN
        EXEC [monitor].[usp_ProcedureRunStart]
            @PipelineRunId = @NormalizedPipelineRunId,
            @ProcedureName = N'stage.usp_ProcessUsers',
            @ProcedurePhase = N'Stage';
    END;

    BEGIN TRY
        BEGIN TRAN;

        SET @Sql = N'TRUNCATE TABLE ' + @StageUsersTable + N';';
        EXEC (@Sql);

        SET @Sql = N'
            INSERT INTO ' + @StageUsersTable + N'
            (
                [FirstName], [LastName], [Email], [Username], [DateOfBirth], [RegistrationDate],
                [Country], [City], [Gender], [AccountCreatedVia], [ReferralSource], [SubscriptionTier],
                [BillingCycle], [PaymentMethod], [AutoRenew], [MarketingConsent], [PreferredLanguage],
                [ContentLanguage], [PlanAddons], [LastRefreshedDate]
            )
            SELECT
                r.[FirstName], r.[LastName], r.[Email], r.[Username], r.[DateOfBirth], r.[RegistrationDate],
                r.[Country], r.[City], r.[Gender], r.[AccountCreatedVia], r.[ReferralSource], r.[SubscriptionTier],
                r.[BillingCycle], r.[PaymentMethod],
                CASE WHEN r.[AutoRenew] = ''1'' THEN ''Yes'' ELSE ''No'' END,
                CASE WHEN r.[MarketingConsent] = ''1'' THEN ''Yes'' ELSE ''No'' END,
                r.[PreferredLanguage], r.[ContentLanguage], r.[PlanAddons], CURRENT_TIMESTAMP
            FROM ' + @RawUsersTable + N' AS r;
            SET @OutRows = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            N'@OutRows BIGINT OUTPUT',
            @OutRows = @RowsRead OUTPUT;

        SET @RowsWritten = @RowsRead;
        SET @RowsInserted = @RowsRead;

        SET @Sql = N'
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
            FROM ' + @StageUsersTable + N' AS s;
            SET @OutRows = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            N'@OutRows BIGINT OUTPUT',
            @OutRows = @RowsTouched OUTPUT;

        SET @RowsUpdated += @RowsTouched;

        SET @Sql = N'
            UPDATE s
            SET
                s.[FullName] = CONCAT(ISNULL(s.[FirstName], ''''), '' '', ISNULL(s.[LastName], '''')),
                s.[YearOfBirth] = CASE WHEN s.[DateOfBirth] IS NULL THEN NULL ELSE CAST(YEAR(s.[DateOfBirth]) AS VARCHAR(4)) END,
                s.[MonthOfBirth] = CASE WHEN s.[DateOfBirth] IS NULL THEN NULL ELSE DATENAME(MONTH, s.[DateOfBirth]) END,
                s.[DayOfBirth] = CASE WHEN s.[DateOfBirth] IS NULL THEN NULL ELSE RIGHT(CONCAT(''0'', CAST(DAY(s.[DateOfBirth]) AS VARCHAR(2))), 2) END,
                s.[LastRefreshedDate] = CURRENT_TIMESTAMP
            FROM ' + @StageUsersTable + N' AS s;
            SET @OutRows = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            N'@OutRows BIGINT OUTPUT',
            @OutRows = @RowsTouched OUTPUT;

        SET @RowsUpdated += @RowsTouched;

        SET @Sql = N'
            UPDATE s
            SET s.[CountryCode] = c.[CountryCode]
            FROM ' + @StageUsersTable + N' AS s
            LEFT JOIN [config].[Countries] AS c
                ON s.[Country] = c.[CountryName];
            SET @OutRows = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            N'@OutRows BIGINT OUTPUT',
            @OutRows = @RowsTouched OUTPUT;

        SET @RowsUpdated += @RowsTouched;

        SET @Sql = N'
            UPDATE s
            SET
                s.[SubscriptionTierRank] = t.[TierRank],
                s.[IsPaidTier] = CASE WHEN t.[IsPaid] = 1 THEN ''Yes'' WHEN t.[IsPaid] = 0 THEN ''No'' END
            FROM ' + @StageUsersTable + N' AS s
            LEFT JOIN [config].[SubscriptionTiers] AS t
                ON s.[SubscriptionTier] = t.[TierCode];
            SET @OutRows = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            N'@OutRows BIGINT OUTPUT',
            @OutRows = @RowsTouched OUTPUT;

        SET @RowsUpdated += @RowsTouched;

        SET @Sql = N'
            UPDATE s
            SET
                s.[PaymentMethodGroup] = p.[PaymentMethodGroup],
                s.[IsCardBased] = CASE WHEN p.[IsCardBased] = 1 THEN ''Yes'' WHEN p.[IsCardBased] = 0 THEN ''No'' END
            FROM ' + @StageUsersTable + N' AS s
            LEFT JOIN [config].[PaymentMethods] AS p
                ON s.[PaymentMethod] = p.[PaymentMethodCode];
            SET @OutRows = @@ROWCOUNT;';

        EXEC sys.sp_executesql
            @Sql,
            N'@OutRows BIGINT OUTPUT',
            @OutRows = @RowsTouched OUTPUT;

        SET @RowsUpdated += @RowsTouched;

        SET @Sql = N'
            UPDATE s
            SET
                s.[Hashdata] = CONCAT(
                    ISNULL(s.[Email], ''''), ''|'',
                    ISNULL(s.[Username], ''''), ''|'',
                    ISNULL(s.[SubscriptionTier], ''''), ''|'',
                    ISNULL(s.[BillingCycle], ''''), ''|'',
                    ISNULL(s.[PaymentMethod], ''''), ''|'',
                    ISNULL(s.[AutoRenew], ''''), ''|'',
                    ISNULL(s.[MarketingConsent], ''''), ''|'',
                    ISNULL(s.[PreferredLanguage], ''''), ''|'',
                    ISNULL(s.[ContentLanguage], ''''), ''|'',
                    ISNULL(s.[PlanAddons], '''')
                ),
                s.[Rowhash] = HASHBYTES(''SHA2_256'', CONCAT(
                    ISNULL(s.[Email], ''''), ''|'',
                    ISNULL(s.[Username], ''''), ''|'',
                    ISNULL(s.[SubscriptionTier], ''''), ''|'',
                    ISNULL(s.[BillingCycle], ''''), ''|'',
                    ISNULL(s.[PaymentMethod], ''''), ''|'',
                    ISNULL(s.[AutoRenew], ''''), ''|'',
                    ISNULL(s.[MarketingConsent], ''''), ''|'',
                    ISNULL(s.[PreferredLanguage], ''''), ''|'',
                    ISNULL(s.[ContentLanguage], ''''), ''|'',
                    ISNULL(s.[PlanAddons], '''')
                ))
            FROM ' + @StageUsersTable + N' AS s;';

        EXEC (@Sql);

        SET @RowsScanned = @RowsRead + @RowsUpdated;

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
                @ProcedureName = N'stage.usp_ProcessUsers',
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

        IF @RowsScanned = 0
        BEGIN
            SET @RowsScanned = @RowsRead + @RowsUpdated;
        END;

        IF @NormalizedPipelineRunId IS NOT NULL
        BEGIN
            BEGIN TRY
                EXEC [monitor].[usp_ProcedureRunFinish]
                    @PipelineRunId = @NormalizedPipelineRunId,
                    @ProcedureName = N'stage.usp_ProcessUsers',
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
