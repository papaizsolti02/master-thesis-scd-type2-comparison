-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: prod.usp_MergeUsers
-- Description: SCD2 merge from stage.Users to prod.Users using update + insert.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [prod].[usp_MergeUsers] -- noqa: 
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @AsOfDate DATETIME = CURRENT_TIMESTAMP;
    DECLARE @MatchedCount INT = 0;
    DECLARE @ExpiredCount INT = 0;
    DECLARE @InsertedCount INT = 0;

    IF OBJECT_ID(N'[stage].[Users]', N'U') IS NULL
    BEGIN
        THROW 50101, 'Required table [stage].[Users] does not exist.', 1;
    END;

    IF OBJECT_ID(N'[prod].[Users]', N'U') IS NULL
    BEGIN
        THROW 50102, 'Required table [prod].[Users] does not exist.', 1;
    END;

    BEGIN TRY
        BEGIN TRAN;

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

        UPDATE p
        SET
            p.[LastRefreshedDate] = @AsOfDate
        FROM
            [prod].[Users] AS p;

        COMMIT TRAN;

        SELECT
            @AsOfDate AS [MergeTimestamp],
            @MatchedCount AS [MatchedRows],
            @ExpiredCount AS [ExpiredRows],
            @InsertedCount AS [InsertedRows];
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
