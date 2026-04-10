CREATE PROCEDURE [stg].[usp_Transform_Users_Raw]
    @BatchId NVARCHAR(64)
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM [stg].[Users_Clean]
    WHERE [BatchId] = @BatchId;

    INSERT INTO [stg].[Users_Clean]
    (
        [UserID],
        [FirstName],
        [LastName],
        [FullName],
        [EmailNormalized],
        [UsernameNormalized],
        [DateOfBirth],
        [RegistrationDate],
        [CountryNormalized],
        [CityNormalized],
        [GenderNormalized],
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
        [TenureDays],
        [HashDiff],
        [BatchId]
    )
    SELECT
        r.[UserID],
        TRIM(ISNULL(r.[FirstName], '')) AS [FirstName],
        TRIM(ISNULL(r.[LastName], '')) AS [LastName],
        CONCAT(TRIM(ISNULL(r.[FirstName], '')), N' ', TRIM(ISNULL(r.[LastName], ''))) AS [FullName],
        LOWER(TRIM(ISNULL(r.[Email], ''))) AS [EmailNormalized],
        LOWER(TRIM(ISNULL(r.[Username], ''))) AS [UsernameNormalized],
        r.[DateOfBirth],
        r.[RegistrationDate],
        UPPER(TRIM(r.[Country])) AS [CountryNormalized],
        TRIM(r.[City]) AS [CityNormalized],
        UPPER(TRIM(r.[Gender])) AS [GenderNormalized],
        TRIM(r.[AccountCreatedVia]) AS [AccountCreatedVia],
        TRIM(r.[ReferralSource]) AS [ReferralSource],
        TRIM(r.[SubscriptionTier]) AS [SubscriptionTier],
        TRIM(r.[BillingCycle]) AS [BillingCycle],
        TRIM(r.[PaymentMethod]) AS [PaymentMethod],
        r.[AutoRenew],
        r.[MarketingConsent],
        LOWER(TRIM(r.[PreferredLanguage])) AS [PreferredLanguage],
        LOWER(TRIM(r.[ContentLanguage])) AS [ContentLanguage],
        TRIM(r.[PlanAddons]) AS [PlanAddons],
        r.[TenureDays],
        HASHBYTES(
            'SHA2_256',
            CONCAT(
                TRIM(ISNULL(r.[FirstName], '')), '|',
                TRIM(ISNULL(r.[LastName], '')), '|',
                LOWER(TRIM(ISNULL(r.[Email], ''))), '|',
                LOWER(TRIM(ISNULL(r.[Username], ''))), '|',
                TRIM(ISNULL(r.[Country], '')), '|',
                TRIM(ISNULL(r.[City], '')), '|',
                TRIM(ISNULL(r.[SubscriptionTier], '')), '|',
                TRIM(ISNULL(r.[BillingCycle], '')), '|',
                TRIM(ISNULL(r.[PaymentMethod], '')), '|',
                TRIM(ISNULL(r.[PreferredLanguage], '')), '|',
                TRIM(ISNULL(r.[ContentLanguage], '')), '|',
                TRIM(ISNULL(r.[PlanAddons], ''))
            )
        ) AS [HashDiff],
        @BatchId AS [BatchId]
    FROM [stg].[Users_Raw] AS r
    WHERE r.[BatchId] = @BatchId;
END;
GO
