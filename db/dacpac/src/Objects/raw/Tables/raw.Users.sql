----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: raw.Users
-- Description: Stores raw user snapshot data from the generator.
-- Version: 1.0
----------------------------------------------------------------------
CREATE TABLE [raw].[Users]
(
    [Id] INT IDENTITY (1, 1) PRIMARY KEY,
    [FirstName] NVARCHAR(120) NULL,
    [LastName] NVARCHAR(120) NULL,
    [Email] NVARCHAR(320) NULL,
    [Username] NVARCHAR(120) NULL,
    [DateOfBirth] DATE NULL,
    [RegistrationDate] DATE NULL,
    [Country] NVARCHAR(100) NULL,
    [City] NVARCHAR(120) NULL,
    [Gender] NVARCHAR(20) NULL,
    [AccountCreatedVia] NVARCHAR(40) NULL,
    [ReferralSource] NVARCHAR(40) NULL,
    [SubscriptionTier] NVARCHAR(40) NULL,
    [BillingCycle] NVARCHAR(40) NULL,
    [PaymentMethod] NVARCHAR(40) NULL,
    [AutoRenew] BIT NULL,
    [MarketingConsent] BIT NULL,
    [PreferredLanguage] NVARCHAR(10) NULL,
    [ContentLanguage] NVARCHAR(10) NULL,
    [PlanAddons] NVARCHAR(100) NULL,
);
GO
