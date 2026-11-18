
CREATE TABLE PaymentTransactions
(
    PaymentId UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
    OrderId UNIQUEIDENTIFIER NOT NULL,
    CustomerName NVARCHAR(100) NOT NULL,
    Amount DECIMAL(18,2) NOT NULL,
    PaymentStatus NVARCHAR(30) NOT NULL,
    PaymentReference NVARCHAR(100) NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedAt DATETIME2 NULL
);

-- Prevent multiple successful payment records for the same order.
CREATE UNIQUE INDEX UX_PaymentTransactions_OrderId
ON PaymentTransactions(OrderId);
