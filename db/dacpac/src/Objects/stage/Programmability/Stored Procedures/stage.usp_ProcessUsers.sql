-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: stage.usp_ProcessUsers
-- Description: Truncates stage.Users, loads from raw.Users, and processes data.
-- Version: 1.0
-- -----------------------------------------------------------------------------
CREATE PROCEDURE [stage].[usp_ProcessUsers]
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

    DECLARE @CpuStart BIGINT = NULL;
    DECLARE @CpuEnd BIGINT = NULL;
    DECLARE @LogicalReadsStart BIGINT = NULL;
    DECLARE @LogicalReadsEnd BIGINT = NULL;
    DECLARE @PhysicalReadsStart BIGINT = NULL;
    DECLARE @PhysicalReadsEnd BIGINT = NULL;
    DECLARE @WritesStart BIGINT = NULL;
    DECLARE @WritesEnd BIGINT = NULL;

    DECLARE @CpuDelta BIGINT = NULL;
    DECLARE @LogicalReadsDelta BIGINT = NULL;
    DECLARE @PhysicalReadsDelta BIGINT = NULL;
    DECLARE @WritesDelta BIGINT = NULL;

    DECLARE @ErrorNumber INT = NULL;
    DECLARE @ErrorMessage NVARCHAR(4000) = NULL;

    IF OBJECT_ID(N'[stage].[Users]', N'U') IS NULL
    BEGIN
        THROW 50001, 'Required table [stage].[Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[raw].[Users]', N'U') IS NULL
    BEGIN
        THROW 50002, 'Required table [raw].[Users] does not exist.', 1;
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

    SELECT
        @CpuStart = s.[cpu_time],
        @LogicalReadsStart = s.[logical_reads],
        @PhysicalReadsStart = s.[reads],
        @WritesStart = s.[writes]
    FROM
        [sys].[dm_exec_sessions] AS s
    WHERE
        s.[session_id] = @@SPID;

    IF @NormalizedPipelineRunId IS NOT NULL
    BEGIN
        EXEC [monitor].[usp_ProcedureRunStart]
            @PipelineRunId = @NormalizedPipelineRunId,
            @ProcedureName = N'stage.usp_ProcessUsers',
            @ProcedurePhase = N'Stage';
    END;

    BEGIN TRY
        BEGIN TRAN;

        TRUNCATE TABLE [stage].[Users];

        INSERT INTO [stage].[Users]
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
            CASE WHEN r.[AutoRenew] = '1' THEN 'Yes' ELSE 'No' END AS [AutoRenew],
            CASE WHEN r.[MarketingConsent] = '1' THEN 'Yes' ELSE 'No' END AS [MarketingConsent],
            r.[PreferredLanguage],
            r.[ContentLanguage],
            r.[PlanAddons],
            CURRENT_TIMESTAMP AS [LastRefreshedDate]
        FROM
            [raw].[Users] AS r;

        SET @RowsRead = @@ROWCOUNT;
        SET @RowsWritten = @RowsRead;
        SET @RowsInserted = @RowsRead;

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
        FROM
            [stage].[Users] AS s;

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[FullName] = CONCAT(ISNULL(s.[FirstName], ''), ' ', ISNULL(s.[LastName], '')),
            s.[YearOfBirth] =
            CASE
                WHEN s.[DateOfBirth] IS NULL THEN NULL
                ELSE CAST(YEAR(s.[DateOfBirth]) AS VARCHAR(4))
            END,
            s.[MonthOfBirth] =
            CASE
                WHEN s.[DateOfBirth] IS NULL THEN NULL
                ELSE DATENAME(MONTH, s.[DateOfBirth])
            END,
            s.[DayOfBirth] =
            CASE
                WHEN s.[DateOfBirth] IS NULL THEN NULL
                ELSE RIGHT(CONCAT('0', CAST(DAY(s.[DateOfBirth]) AS VARCHAR(2))), 2)
            END,
            s.[LastRefreshedDate] = CURRENT_TIMESTAMP
        FROM
            [stage].[Users] AS s;

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[CountryCode] = c.[CountryCode]
        FROM
            [stage].[Users] AS s
        LEFT JOIN [config].[Countries] AS c
            ON s.[Country] = c.[CountryName];

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[SubscriptionTierRank] = t.[TierRank],
            s.[IsPaidTier] =
            CASE
                WHEN t.[IsPaid] = 1 THEN 'Yes'
                WHEN t.[IsPaid] = 0 THEN 'No'
            END
        FROM
            [stage].[Users] AS s
        LEFT JOIN [config].[SubscriptionTiers] AS t
            ON s.[SubscriptionTier] = t.[TierCode];

        SET @RowsUpdated += @@ROWCOUNT;

        UPDATE s
        SET
            s.[PaymentMethodGroup] = p.[PaymentMethodGroup],
            s.[IsCardBased] = CASE
                WHEN p.[IsCardBased] = 1 THEN 'Yes'
                WHEN p.[IsCardBased] = 0 THEN 'No'
            END
        FROM
            [stage].[Users] AS s
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
        FROM
            [stage].[Users] AS s;

        SET @RowsScanned = @RowsRead + @RowsUpdated;

        COMMIT TRAN;

        SELECT
            @CpuEnd = s.[cpu_time],
            @LogicalReadsEnd = s.[logical_reads],
            @PhysicalReadsEnd = s.[reads],
            @WritesEnd = s.[writes]
        FROM [sys].[dm_exec_sessions] AS s
        WHERE s.[session_id] = @@SPID;

        SET @CpuDelta = CASE WHEN @CpuStart IS NULL OR @CpuEnd IS NULL THEN NULL ELSE @CpuEnd - @CpuStart END;
        SET @LogicalReadsDelta = CASE WHEN @LogicalReadsStart IS NULL OR @LogicalReadsEnd IS NULL THEN NULL ELSE @LogicalReadsEnd - @LogicalReadsStart END;
        SET @PhysicalReadsDelta = CASE WHEN @PhysicalReadsStart IS NULL OR @PhysicalReadsEnd IS NULL THEN NULL ELSE @PhysicalReadsEnd - @PhysicalReadsStart END;
        SET @WritesDelta = CASE WHEN @WritesStart IS NULL OR @WritesEnd IS NULL THEN NULL ELSE @WritesEnd - @WritesStart END;

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

        SELECT
            @CpuEnd = s.[cpu_time],
            @LogicalReadsEnd = s.[logical_reads],
            @PhysicalReadsEnd = s.[reads],
            @WritesEnd = s.[writes]
        FROM
            [sys].[dm_exec_sessions] AS s
        WHERE
            s.[session_id] = @@SPID;

        SET @CpuDelta = CASE WHEN @CpuStart IS NULL OR @CpuEnd IS NULL THEN NULL ELSE @CpuEnd - @CpuStart END;
        SET @LogicalReadsDelta = CASE WHEN @LogicalReadsStart IS NULL OR @LogicalReadsEnd IS NULL THEN NULL ELSE @LogicalReadsEnd - @LogicalReadsStart END;
        SET @PhysicalReadsDelta = CASE WHEN @PhysicalReadsStart IS NULL OR @PhysicalReadsEnd IS NULL THEN NULL ELSE @PhysicalReadsEnd - @PhysicalReadsStart END;
        SET @WritesDelta = CASE WHEN @WritesStart IS NULL OR @WritesEnd IS NULL THEN NULL ELSE @WritesEnd - @WritesStart END;

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
