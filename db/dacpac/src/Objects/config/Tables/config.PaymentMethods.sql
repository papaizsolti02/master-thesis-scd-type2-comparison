-- ---------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: config.PaymentMethods
-- Description: Stores payment method reference data used for lookup joins.
-- Version: 1.0
-- ---------------------------------------------------------------------------

CREATE TABLE [config].[PaymentMethods]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [PaymentMethodCode] NVARCHAR(40) NOT NULL UNIQUE,
    [PaymentMethodGroup] NVARCHAR(40) NOT NULL,
    [IsCardBased] BIT NOT NULL
);
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Lookup table for payment method groups and card-based classification.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'PaymentMethods';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Surrogate key for the payment method row.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'PaymentMethods',
    @level2type = N'COLUMN', @level2name = N'Id';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Logical code for the payment method.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'PaymentMethods',
    @level2type = N'COLUMN', @level2name = N'PaymentMethodCode';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Grouped payment method label used for reporting.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'PaymentMethods',
    @level2type = N'COLUMN', @level2name = N'PaymentMethodGroup';
GO
EXEC sys.sp_addextendedproperty
    @name = N'MS_Description', @value = N'Indicates whether the payment method is card-based.',
    @level0type = N'SCHEMA', @level0name = N'config',
    @level1type = N'TABLE', @level1name = N'PaymentMethods',
    @level2type = N'COLUMN', @level2name = N'IsCardBased';
GO
