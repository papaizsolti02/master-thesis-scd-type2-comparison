-- -------------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: config.SubscriptionTiers
-- Description: Stores subscription tier reference data used for lookup joins.
-- Version: 1.0
-- -------------------------------------------------------------------------------

CREATE TABLE [config].[SubscriptionTiers]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [TierCode] NVARCHAR(40) NOT NULL UNIQUE,
    [TierName] NVARCHAR(40) NOT NULL UNIQUE,
    [TierRank] TINYINT NOT NULL,
    [IsPaid] BIT NOT NULL
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Lookup table for subscription tiers, pricing rank, and paid-tier flag.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'SubscriptionTiers';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the subscription tier row.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'SubscriptionTiers',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Logical code for the subscription tier.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'SubscriptionTiers',
    @level2type = N'COLUMN', @level2name = N'TierCode';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Display name for the subscription tier.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'SubscriptionTiers',
    @level2type = N'COLUMN', @level2name = N'TierName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Ordering rank used in reporting and enrichment.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'SubscriptionTiers',
    @level2type = N'COLUMN', @level2name = N'TierRank';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Indicates whether the tier is paid.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'SubscriptionTiers',
    @level2type = N'COLUMN', @level2name = N'IsPaid';
GO
