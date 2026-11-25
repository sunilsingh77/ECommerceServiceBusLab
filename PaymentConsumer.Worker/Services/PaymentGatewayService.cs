using Microsoft.EntityFrameworkCore;
using PaymentConsumer.Worker.Data;
using PaymentConsumer.Worker.Exceptions;
using PaymentConsumer.Worker.Models;

namespace PaymentConsumer.Worker.Services;

public sealed class PaymentGatewayService
{
    private readonly PaymentDbContext _dbContext;
    private readonly ILogger<PaymentGatewayService> _logger;

    public PaymentGatewayService(
        PaymentDbContext dbContext,
        ILogger<PaymentGatewayService> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    public async Task ProcessPaymentAsync(
        Guid orderId,
        string customerName,
        decimal amount,
        string? correlationId,
        CancellationToken cancellationToken)
    {
        // Idempotency guard: do not attempt a second successful payment
        // for the same OrderId.
        var existingPayment = await _dbContext.PaymentTransactions
            .AsNoTracking()
            .FirstOrDefaultAsync(
                payment => payment.OrderId == orderId,
                cancellationToken);

        if (existingPayment is not null)
        {
            if (existingPayment.PaymentStatus == "Succeeded")
            {
                _logger.LogWarning(
                    "Duplicate payment request detected for OrderId {OrderId}; skipping gateway call",
                    orderId);

                return;
            }

            throw new InvalidOperationException(
                $"Order {orderId} already has a payment with status {existingPayment.PaymentStatus}.");
        }

        // Keep the existing permanent-failure test scenario.
        if (customerName.Equals(
                "INVALID",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new PermanentPaymentException("Invalid payment details.");
        }

        // TEMPFAIL simulation remains disabled as in the supplied source.
        // Uncomment this block when you want to test the retry path.
        //
        // if (customerName.Equals("TEMPFAIL", StringComparison.OrdinalIgnoreCase))
        // {
        //     throw new TransientPaymentException(
        //         "Payment gateway temporarily unavailable (503).");
        // }

        _logger.LogInformation(
            "Calling payment gateway simulation. IdempotencyKey: {OrderId}",
            orderId);

        await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

        var now = DateTime.UtcNow;

        var payment = new PaymentTransaction
        {
            PaymentId = Guid.NewGuid(),
            OrderId = orderId,
            CustomerName = customerName,
            Amount = amount,
            PaymentStatus = "Succeeded",
            PaymentReference = $"PAY-{Guid.NewGuid():N}",
            CreatedAt = now,
            UpdatedAt = now,
            CorrelationId = correlationId
        };

        _dbContext.PaymentTransactions.Add(payment);

        try
        {
            // This SaveChanges participates in the outer transaction
            // opened by PaymentProcessor because the same DbContext is used.
            await _dbContext.SaveChangesAsync(cancellationToken);

            _logger.LogInformation(
                "Payment transaction saved. OrderId: {OrderId}, PaymentId: {PaymentId}",
                orderId,
                payment.PaymentId);
        }
        catch (DbUpdateException ex)
        {
            // A concurrent delivery may have won the unique OrderId constraint.
            // The caller's surrounding transaction will be rolled back if this
            // is not a confirmed successful duplicate.
            _logger.LogWarning(
                ex,
                "Payment insert encountered a database update conflict. Checking existing payment for OrderId {OrderId}",
                orderId);

            var duplicate = await _dbContext.PaymentTransactions
                .AsNoTracking()
                .FirstOrDefaultAsync(
                    existing => existing.OrderId == orderId,
                    cancellationToken);

            if (duplicate?.PaymentStatus == "Succeeded")
            {
                _logger.LogWarning(
                    "Duplicate payment prevented by database unique constraint. OrderId: {OrderId}",
                    orderId);

                return;
            }

            throw;
        }
    }
}
