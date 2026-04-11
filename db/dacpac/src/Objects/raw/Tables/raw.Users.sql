-- ----------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: raw.Users
-- Description: Stores raw user snapshot data from the generator.
-- Version: 1.0
-- ----------------------------------------------------------------------

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
    [AutoRenew] CHAR(1) NULL,
    [MarketingConsent] CHAR(1) NULL,
    [PreferredLanguage] NVARCHAR(10) NULL,
    [ContentLanguage] NVARCHAR(10) NULL,
    [PlanAddons] NVARCHAR(100) NULL
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Raw landing table for imported user snapshot data.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the raw user row.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Given name of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'FirstName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Family name of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'LastName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Email address of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Email';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Login username of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Username';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Birth date of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'DateOfBirth';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Registration date of the user account.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'RegistrationDate';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Country name or code from the source snapshot.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Country';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'City of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'City';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Gender of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'Gender';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Channel through which the account was created.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'AccountCreatedVia';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Source of user referral.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ReferralSource';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Subscription tier selected by the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'SubscriptionTier';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Billing cycle selected by the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'BillingCycle';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Payment method selected by the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PaymentMethod';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Raw auto-renew flag from the source snapshot.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'AutoRenew';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Raw marketing-consent flag from the source snapshot.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'MarketingConsent';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Preferred language of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PreferredLanguage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Content language preference of the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'ContentLanguage';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Add-on package selected by the user.',
    @level0type = N'SCHEMA', @level0name = N'raw',
    @level1type = N'TABLE', @level1name = N'Users',
    @level2type = N'COLUMN', @level2name = N'PlanAddons';
GO
