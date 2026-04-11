-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: stage.Users
-- Description: Stores transformed user data and staging metadata.
-- Version: 1.0
-- ----------------------------------------------------------------------

CREATE TABLE [stage].[Users]
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

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Staging table for normalized and enriched user data.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the staged user row.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Concatenated full name used for reporting.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'FullName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Given name after trimming and normalization.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'FirstName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Family name after trimming and normalization.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'LastName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Lower-cased normalized email address.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Email';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Lower-cased normalized username.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Username';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth date of the staged user.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'DateOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth year derived from the date of birth.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'YearOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth month derived from the date of birth.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'MonthOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth day derived from the date of birth.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'DayOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Registration date of the user.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'RegistrationDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized country name.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Country';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Country code looked up from the config table.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'CountryCode';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized city name.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'City';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized gender value.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Gender';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized account creation channel.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'AccountCreatedVia';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized referral source.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ReferralSource';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized subscription tier.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'SubscriptionTier';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Lookup rank for the subscription tier.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'SubscriptionTierRank';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Indicator showing whether the tier is paid.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'IsPaidTier';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized billing cycle.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'BillingCycle';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized payment method.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PaymentMethod';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Payment method group looked up from config.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PaymentMethodGroup';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Indicator showing whether the payment method is card-based.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'IsCardBased';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized auto-renew indicator.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'AutoRenew';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized marketing-consent indicator.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'MarketingConsent';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized preferred language.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PreferredLanguage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Normalized content language.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ContentLanguage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Add-on package assigned to the user.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PlanAddons';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Timestamp when the staged row was last refreshed.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'LastRefreshedDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Concatenated natural-key hash input used to detect change.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Hashdata';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'SHA2_256 hash of the tracked business columns.',
    @level0type = N'SCHEMA', @level0name = N'stage',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Rowhash';
GO
