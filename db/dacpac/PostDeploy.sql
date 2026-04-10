----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-11
-- Name: postdeploy.config.seeds
-- Description: Idempotent seed upsert for config lookup tables.
-- Version: 1.0
----------------------------------------------------------------------------

MERGE [config].[Countries] AS [target]
USING
(
    VALUES
        ('DE', 'Germany', 'de'),
        ('FR', 'France', 'fr'),
        ('GB', 'United Kingdom', 'en'),
        ('IT', 'Italy', 'it'),
        ('ES', 'Spain', 'es'),
        ('NL', 'Netherlands', 'nl'),
        ('PL', 'Poland', 'pl'),
        ('RO', 'Romania', 'ro')
) AS [source] ([CountryCode], [CountryName], [PreferredLanguage])
ON [target].[CountryCode] = [source].[CountryCode]
WHEN MATCHED THEN
    UPDATE
    SET [CountryName] = [source].[CountryName],
        [PreferredLanguage] = [source].[PreferredLanguage]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([CountryCode], [CountryName], [PreferredLanguage])
    VALUES ([source].[CountryCode], [source].[CountryName], [source].[PreferredLanguage]);

MERGE [config].[SubscriptionTiers] AS [target]
USING
(
    VALUES
        ('Free', 'Free', 1, 0),
        ('Basic', 'Basic', 2, 1),
        ('Premium', 'Premium', 3, 1)
) AS [source] ([TierCode], [TierName], [TierRank], [IsPaid])
ON [target].[TierCode] = [source].[TierCode]
WHEN MATCHED THEN
    UPDATE
    SET [TierName] = [source].[TierName],
        [TierRank] = [source].[TierRank],
        [IsPaid] = [source].[IsPaid]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([TierCode], [TierName], [TierRank], [IsPaid])
    VALUES ([source].[TierCode], [source].[TierName], [source].[TierRank], [source].[IsPaid]);

MERGE [config].[PaymentMethods] AS [target]
USING
(
    VALUES
        ('Card', 'Card', 1),
        ('PayPal', 'Digital Wallet', 0),
        ('BankTransfer', 'Bank Transfer', 0)
) AS [source] ([PaymentMethodCode], [PaymentMethodGroup], [IsCardBased])
ON [target].[PaymentMethodCode] = [source].[PaymentMethodCode]
WHEN MATCHED THEN
    UPDATE
    SET [PaymentMethodGroup] = [source].[PaymentMethodGroup],
        [IsCardBased] = [source].[IsCardBased]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([PaymentMethodCode], [PaymentMethodGroup], [IsCardBased])
    VALUES ([source].[PaymentMethodCode], [source].[PaymentMethodGroup], [source].[IsCardBased]);
