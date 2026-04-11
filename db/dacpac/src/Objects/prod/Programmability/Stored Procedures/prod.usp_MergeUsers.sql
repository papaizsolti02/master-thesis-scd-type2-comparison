-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: prod.usp_MergeUsers
-- Description: SCD2 merge from stage.Users to prod.Users using update + insert.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [prod].[usp_MergeUsers] -- noqa: 
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
        THROW 50101, 'Required table [stage].[Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[prod].[Users]', N'U') IS NULL
    BEGIN
        THROW 50102, 'Required table [prod].[Users] does not exist.', 1;
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
