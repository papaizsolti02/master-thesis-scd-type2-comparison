-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: prod.Users
-- Description: Stores SCD type 2 user history data.
-- Version: 1.0
-- ----------------------------------------------------------------------

CREATE TABLE [prod].[Users]
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

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Stores SCD type 2 user history data.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the historical user row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Concatenated full name used for reporting.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'FullName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Given name stored in the current historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'FirstName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Family name stored in the current historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'LastName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Email address of the user.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Email';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Username of the user.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Username';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth date of the user.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'DateOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth year derived from the date of birth.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'YearOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth month derived from the date of birth.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'MonthOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth day derived from the date of birth.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'DayOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Registration date of the account.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'RegistrationDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Country name stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Country';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Country code resolved from the config lookup.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'CountryCode';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'City stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'City';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Gender stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Gender';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Account creation channel.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'AccountCreatedVia';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Referral source stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ReferralSource';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Subscription tier of the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'SubscriptionTier';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Rank of the subscription tier.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'SubscriptionTierRank';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Indicator showing whether the tier is paid.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'IsPaidTier';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Billing cycle stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'BillingCycle';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Payment method stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PaymentMethod';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Payment method group resolved from config.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PaymentMethodGroup';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Indicator showing whether the payment method is card-based.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'IsCardBased';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Auto-renew flag stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'AutoRenew';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Marketing-consent flag stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'MarketingConsent';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Preferred language stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PreferredLanguage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Content language stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ContentLanguage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Add-on package stored in the historical row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PlanAddons';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Timestamp of the last refresh in the production table.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'LastRefreshedDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Effective timestamp for the SCD type 2 row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'EffectiveDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Expiration timestamp for the SCD type 2 row.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ExpirationDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Flag indicating whether the row is currently active.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'IsActive';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Concatenated natural-key hash input used to detect change.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Hashdata';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'SHA2_256 hash of the tracked business columns.',
    @level0type = N'SCHEMA', @level0name = N'prod',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Rowhash';
GO
