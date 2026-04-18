-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-14
-- Name: snapshot_scd2.prod_Users
-- Description: Method-isolated production history table for Snapshot SCD2 tests.
-- Version: 1.0
-- ----------------------------------------------------------------------

CREATE TABLE [snapshot_scd2].[prod_Users]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [FullName] NVARCHAR(241) NULL,
    [FirstName] NVARCHAR(120) NULL,
    [LastName] NVARCHAR(120) NULL,
    [Email] NVARCHAR(320) NULL,
    [Username] NVARCHAR(120) NULL,
    [DateOfBirth] DATE NULL,
    [YearOfBirth] VARCHAR(4) NULL,
    [MonthOfBirth] VARCHAR(20) NULL,
    [DayOfBirth] VARCHAR(2) NULL,
    [RegistrationDate] DATE NULL,
    [Country] NVARCHAR(100) NULL,
    [CountryCode] CHAR(2) NULL,
    [City] NVARCHAR(120) NULL,
    [Gender] NVARCHAR(20) NULL,
    [AccountCreatedVia] NVARCHAR(40) NULL,
    [ReferralSource] NVARCHAR(40) NULL,
    [SubscriptionTier] NVARCHAR(40) NULL,
    [SubscriptionTierRank] TINYINT NULL,
    [IsPaidTier] NVARCHAR(3) NULL,
    [BillingCycle] NVARCHAR(40) NULL,
    [PaymentMethod] NVARCHAR(40) NULL,
    [PaymentMethodGroup] NVARCHAR(40) NULL,
    [IsCardBased] NVARCHAR(3) NULL,
    [AutoRenew] NVARCHAR(3) NULL,
    [MarketingConsent] NVARCHAR(3) NULL,
    [PreferredLanguage] NVARCHAR(10) NULL,
    [ContentLanguage] NVARCHAR(10) NULL,
    [PlanAddons] NVARCHAR(100) NULL,
    [LastRefreshedDate] DATETIME NULL,
    [EffectiveDate] DATETIME NULL,
    [ExpirationDate] DATETIME NULL,
    [IsActive] BIT NULL,
    [Hashdata] VARCHAR(MAX) NULL,
    [Rowhash] VARBINARY(6000) NULL
);
GO

CREATE NONCLUSTERED INDEX [IX_snapshot_scd2_prod_Users_Email_Username]
    ON [snapshot_scd2].[prod_Users] ([Email], [Username])
    INCLUDE ([Rowhash], [IsActive]);
GO

CREATE INDEX [IX_PROD_Users_Email_Username_Active]
    ON [snapshot_scd2].[prod_Users] ([Email], [Username])
    WHERE [IsActive] = 1;
GO
