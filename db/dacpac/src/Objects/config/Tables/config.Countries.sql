-- ----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: config.Countries
-- Description: Stores country reference data used for lookup joins.
-- Version: 1.0
-- ----------------------------------------------------------------------------

CREATE TABLE [config].[Countries]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [CountryCode] CHAR(2) NOT NULL UNIQUE,
    [CountryName] NVARCHAR(100) NOT NULL UNIQUE,
    [PreferredLanguage] NVARCHAR(10) NOT NULL
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Lookup table for country codes, country names, and preferred language.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'Countries';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the country lookup row.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'Countries',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Two-letter country code used by the generator and warehouse.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'Countries',
    @level2type = N'COLUMN', @level2name = N'CountryCode';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Human-readable country name.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'Countries',
    @level2type = N'COLUMN', @level2name = N'CountryName';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Default preferred language for the country.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'Countries',
    @level2type = N'COLUMN', @level2name = N'PreferredLanguage';
GO
