using System.Text.Json;
using ECommerce.Contracts;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OrderProducer.Api.Data;
using OrderProducer.Api.Models;

namespace OrderProducer.Api.Controllers;

[ApiController]
[Route("api/orders")]
public class OrdersController : ControllerBase
{
    private readonly OrderDbContext _dbContext;
    private readonly ILogger<OrdersController> _logger;

    public OrdersController(
        OrderDbContext dbContext,
        ILogger<OrdersController> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(
        [FromQuery] string customerName = "John",
        [FromQuery] Guid? orderId = null,
        CancellationToken cancellationToken = default)
    {
        var correlationId =
            Request.Headers["X-Correlation-ID"].FirstOrDefault();

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            correlationId = Guid.NewGuid().ToString("N");
        }

        var orderEvent = new OrderCreatedEvent
        {
            OrderId = orderId ?? Guid.NewGuid(),
            CustomerName = customerName,
            TotalAmount = 2500,
            CreatedAt = DateTime.UtcNow
        };

        await using var transaction =
            await _dbContext.Database.BeginTransactionAsync(
                cancellationToken);

        try
        {
            var order = new Order
            {
                OrderId = orderEvent.OrderId,
                CustomerName = orderEvent.CustomerName,
                TotalAmount = orderEvent.TotalAmount,
                CreatedAt = orderEvent.CreatedAt,
                CorrelationId = correlationId
            };

            _dbContext.Orders.Add(order);

            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = nameof(OrderCreatedEvent),
                Content = JsonSerializer.Serialize(orderEvent),
                OccurredOnUtc = DateTime.UtcNow,
                CorrelationId = correlationId
            };

            _dbContext.OutboxMessages.Add(outboxMessage);

            await _dbContext.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            _logger.LogInformation(
                "Order {OrderId} saved to Outbox. EventId: {EventId}, CorrelationId: {CorrelationId}",
                order.OrderId,
                outboxMessage.Id,
                correlationId);

            return Accepted(new
            {
                Message = "Order created and event stored in Outbox.",
                order.OrderId,
                EventId = outboxMessage.Id,
                CorrelationId = correlationId,
                Status = "PendingPublication"
            });
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }
}