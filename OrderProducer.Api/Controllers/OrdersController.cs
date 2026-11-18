
using ECommerce.Contracts;
using Microsoft.AspNetCore.Mvc;
using OrderProducer.Api.Services;

namespace OrderProducer.Api.Controllers;

[ApiController]
[Route("api/orders")]
public class OrdersController : ControllerBase
{
    private readonly ServiceBusPublisher _publisher;

    public OrdersController(ServiceBusPublisher publisher)
    {
        _publisher = publisher;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromQuery] string customerName = "John", Guid? orderId = null, CancellationToken cancellationToken = default)
    {
        var order = new OrderCreatedEvent
        {
            OrderId = orderId ?? Guid.NewGuid(),
            CustomerName = customerName,
            TotalAmount = 2500,
            CreatedAt = DateTime.UtcNow
        };

        //await _publisher.PublishOrderToQueueAsync(order, cancellationToken);

        await _publisher.PublishOrderToTopicAsync(order, cancellationToken);

        return Accepted(new
        {
            Message = "Order submitted successfully",
            order.OrderId
        });
    }
}
