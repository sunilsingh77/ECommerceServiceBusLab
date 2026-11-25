
using Microsoft.EntityFrameworkCore;
using PaymentConsumer.Worker.Data;
using PaymentConsumer.Worker.Exceptions;
using PaymentConsumer.Worker.Models;

namespace PaymentConsumer.Worker.Services;

public class PaymentGatewayService
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
        // Step 1: Check whether payment already exists.
        var existingPayment = await _dbContext.PaymentTransactions
            .AsNoTracking()
            .FirstOrDefaultAsync(
                x => x.OrderId == orderId,
                cancellationToken);

        if (existingPayment is not null)
        {
            if (existingPayment.PaymentStatus == "Succeeded")
            {
                _logger.LogWarning(
                    "Duplicate payment request detected for Order: {OrderId}. Skipping charge.",
                    orderId);

                return;
            }

            throw new InvalidOperationException(
                $"Order {orderId} already has a payment with status {existingPayment.PaymentStatus}.");
        }

        // Step 2: Simulate payment gateway failures.
        //if (customerName.Equals(
        //    "TEMPFAIL",
        //    StringComparison.OrdinalIgnoreCase))
        //{
        //    throw new TransientPaymentException(
        //        "Payment gateway temporarily unavailable (503).");
        //}

        if (customerName.Equals(
            "INVALID",
            StringComparison.OrdinalIgnoreCase))
        {
            throw new PermanentPaymentException(
                "Invalid payment details.");
        }

        // Step 3: Simulate the payment gateway operation.
        _logger.LogInformation(
            "Calling payment gateway. IdempotencyKey: {OrderId}",
            orderId);

        await Task.Delay(500, cancellationToken);

        // Step 4: Save successful payment record.
        var payment = new PaymentTransaction
        {
            PaymentId = Guid.NewGuid(),
            OrderId = orderId,
            CustomerName = customerName,
            Amount = amount,
            PaymentStatus = "Succeeded",
            PaymentReference = $"PAY-{Guid.NewGuid():N}",
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            CorrelationId = correlationId
        };

        _dbContext.PaymentTransactions.Add(payment);

        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Payment saved successfully. OrderId: {OrderId}, PaymentId: {PaymentId}", orderId, payment.PaymentId);
        }
        catch (DbUpdateException)
        {
            // Another concurrent request may have inserted this OrderId.
            // Verify whether the payment already exists.
            var duplicate = await _dbContext.PaymentTransactions
                .AsNoTracking()
                .FirstOrDefaultAsync(
                    x => x.OrderId == orderId,
                    cancellationToken);

            if (duplicate?.PaymentStatus == "Succeeded")
            {
                _logger.LogWarning("Duplicate payment prevented by database unique constraint. OrderId: {OrderId}", orderId);
                return;
            }
            throw;
        }
    }
}
