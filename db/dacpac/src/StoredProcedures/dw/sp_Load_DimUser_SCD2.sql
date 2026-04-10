CREATE PROCEDURE [dw].[usp_Load_DimUser_SCD2]
    @BatchId NVARCHAR(64)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE d
    SET d.[IsCurrent] = 0,
        d.[EffectiveTo] = SYSUTCDATETIME()
    FROM [dw].[DimUser] AS d
    INNER JOIN [stg].[Users_Clean] AS s
        ON d.[UserID] = s.[UserID]
       AND d.[IsCurrent] = 1
    WHERE s.[BatchId] = @BatchId
      AND d.[HashDiff] <> s.[HashDiff];

    INSERT INTO [dw].[DimUser]
    (
        [UserID],
        [FirstName],
        [LastName],
        [FullName],
        [EmailNormalized],
        [UsernameNormalized],
        [CountryNormalized],
        [CityNormalized],
        [SubscriptionTier],
        [BillingCycle],
        [PaymentMethod],
        [PreferredLanguage],
        [ContentLanguage],
        [PlanAddons],
        [HashDiff],
        [EffectiveFrom],
        [EffectiveTo],
        [IsCurrent],
        [BatchId]
    )
    SELECT
        s.[UserID],
        s.[FirstName],
        s.[LastName],
        s.[FullName],
        s.[EmailNormalized],
        s.[UsernameNormalized],
        s.[CountryNormalized],
        s.[CityNormalized],
        s.[SubscriptionTier],
        s.[BillingCycle],
        s.[PaymentMethod],
        s.[PreferredLanguage],
        s.[ContentLanguage],
        s.[PlanAddons],
        s.[HashDiff],
        SYSUTCDATETIME() AS [EffectiveFrom],
        NULL AS [EffectiveTo],
        1 AS [IsCurrent],
        @BatchId AS [BatchId]
    FROM [stg].[Users_Clean] AS s
    LEFT JOIN [dw].[DimUser] AS d
        ON s.[UserID] = d.[UserID]
       AND d.[IsCurrent] = 1
    WHERE s.[BatchId] = @BatchId
      AND (d.[UserID] IS NULL OR d.[HashDiff] <> s.[HashDiff]);
END;
GO
