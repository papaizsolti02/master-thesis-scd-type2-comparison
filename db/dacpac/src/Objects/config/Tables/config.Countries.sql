----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: config.Countries
-- Description: Stores country reference data used for lookup joins.
-- Version: 1.0
----------------------------------------------------------------------------
CREATE TABLE [config].[Countries]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [CountryCode] CHAR(2) NOT NULL UNIQUE,
    [CountryName] NVARCHAR(100) NOT NULL UNIQUE,
    [PreferredLanguage] NVARCHAR(10) NOT NULL
);
GO

INSERT INTO [config].[Countries] ([CountryCode], [CountryName], [PreferredLanguage])
VALUES
    ('DE', 'Germany', 'de'),
    ('FR', 'France', 'fr'),
    ('GB', 'United Kingdom', 'en'),
    ('IT', 'Italy', 'it'),
    ('ES', 'Spain', 'es'),
    ('NL', 'Netherlands', 'nl'),
    ('PL', 'Poland', 'pl'),
    ('RO', 'Romania', 'ro');
GO
