-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-14
-- Name: full_scd2.stage_Users
-- Description: Method-isolated staging table for Full SCD2 tests.
-- Version: 1.0
-- ----------------------------------------------------------------------

CREATE TABLE [full_scd2].[stage_Users]
(
    [Id] INT IDENTITY (1, 1) PRIMARY KEY,
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
    [Hashdata] VARCHAR(MAX) NULL,
    [Rowhash] VARBINARY(6000) NULL
);
GO
