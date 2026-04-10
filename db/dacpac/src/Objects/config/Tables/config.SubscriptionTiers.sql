-------------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: config.SubscriptionTiers
-- Description: Stores subscription tier reference data used for lookup joins.
-- Version: 1.0
-------------------------------------------------------------------------------
CREATE TABLE [config].[SubscriptionTiers]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [TierCode] NVARCHAR(40) NOT NULL UNIQUE,
    [TierName] NVARCHAR(40) NOT NULL UNIQUE,
    [TierRank] TINYINT NOT NULL,
    [IsPaid] BIT NOT NULL
);
GO
