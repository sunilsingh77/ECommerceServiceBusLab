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
        Guid? orderId = null,
        CancellationToken cancellationToken = default)
    {
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
            // 1. Save business data.
            var order = new Order
            {
                OrderId = orderEvent.OrderId,
                CustomerName = orderEvent.CustomerName,
                TotalAmount = orderEvent.TotalAmount,
                CreatedAt = orderEvent.CreatedAt
            };

            _dbContext.Orders.Add(order);

            // 2. Save outgoing event in Outbox.
            var outboxMessage = new OutboxMessage
            {
                Id = Guid.NewGuid(),
                Type = nameof(OrderCreatedEvent),
                Content = JsonSerializer.Serialize(orderEvent),
                OccurredOnUtc = DateTime.UtcNow
            };

            _dbContext.OutboxMessages.Add(outboxMessage);

            // 3. Persist both records.
            await _dbContext.SaveChangesAsync(cancellationToken);

            // 4. Commit both records together.
            await transaction.CommitAsync(cancellationToken);

            _logger.LogInformation(
                "Order {OrderId} and Outbox event {EventId} saved successfully.",
                order.OrderId,
                outboxMessage.Id);

            return Accepted(new
            {
                Message = "Order created and event stored in Outbox.",
                order.OrderId,
                EventId = outboxMessage.Id,
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