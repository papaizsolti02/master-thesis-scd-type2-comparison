-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-14
-- Name: snapshot_scd2.TodayComparableUsers
-- Description: Today comparable rows that passed snapshot delta detection.
-- Version: 1.0
-- ----------------------------------------------------------------------

CREATE TABLE [snapshot_scd2].[TodayComparableUsers]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [Email] NVARCHAR(320) NULL,
    [Username] NVARCHAR(120) NULL,
    [SubscriptionTier] NVARCHAR(40) NULL,
    [BillingCycle] NVARCHAR(40) NULL,
    [PaymentMethod] NVARCHAR(40) NULL,
    [AutoRenew] CHAR(1) NULL,
    [MarketingConsent] CHAR(1) NULL,
    [PreferredLanguage] NVARCHAR(10) NULL,
    [ContentLanguage] NVARCHAR(10) NULL,
    [PlanAddons] NVARCHAR(100) NULL,
    [Hashdata] VARCHAR(MAX) NULL,
    [Rowhash] VARBINARY(6000) NULL,
    [LastRefreshedDate] DATETIME NULL
);
GO

CREATE NONCLUSTERED INDEX [IX_snapshot_scd2_TodayComparableUsers_Rowhash]
    ON [snapshot_scd2].[TodayComparableUsers] ([Rowhash]);
GO
