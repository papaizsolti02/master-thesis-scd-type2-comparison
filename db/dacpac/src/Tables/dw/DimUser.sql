CREATE TABLE [dw].[DimUser]
(
    [DimUserKey] BIGINT IDENTITY (1, 1) NOT NULL,
    [UserID] BIGINT NOT NULL,
    [FirstName] NVARCHAR(120) NOT NULL,
    [LastName] NVARCHAR(120) NOT NULL,
    [FullName] NVARCHAR(260) NOT NULL,
    [EmailNormalized] NVARCHAR(320) NOT NULL,
    [UsernameNormalized] NVARCHAR(120) NOT NULL,
    [CountryNormalized] NVARCHAR(100) NULL,
    [CityNormalized] NVARCHAR(120) NULL,
    [SubscriptionTier] NVARCHAR(40) NULL,
    [BillingCycle] NVARCHAR(40) NULL,
    [PaymentMethod] NVARCHAR(40) NULL,
    [PreferredLanguage] NVARCHAR(10) NULL,
    [ContentLanguage] NVARCHAR(10) NULL,
    [PlanAddons] NVARCHAR(100) NULL,
    [HashDiff] VARBINARY(32) NOT NULL,
    [EffectiveFrom] DATETIME2(3) NOT NULL,
    [EffectiveTo] DATETIME2(3) NULL,
    [IsCurrent] BIT NOT NULL,
    [BatchId] NVARCHAR(64) NULL,
    [CreatedAt] DATETIME2(3) NOT NULL CONSTRAINT [DF_DimUser_CreatedAt] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [PK_DimUser] PRIMARY KEY CLUSTERED ([DimUserKey] ASC)
);
GO

CREATE INDEX [IX_DimUser_UserID_IsCurrent]
    ON [dw].[DimUser] ([UserID], [IsCurrent])
    INCLUDE ([HashDiff], [EffectiveFrom], [EffectiveTo]);
GO
