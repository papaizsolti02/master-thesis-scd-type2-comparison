---------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-10
-- Name: config.PaymentMethods
-- Description: Stores payment method reference data used for lookup joins.
-- Version: 1.0
---------------------------------------------------------------------------
CREATE TABLE [config].[PaymentMethods]
(
    [Id] INT IDENTITY (1, 1) NOT NULL PRIMARY KEY,
    [PaymentMethodCode] NVARCHAR(40) NOT NULL UNIQUE,
    [PaymentMethodGroup] NVARCHAR(40) NOT NULL,
    [IsCardBased] BIT NOT NULL
);
GO
