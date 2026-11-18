
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;

namespace OrderProducer.Api.Services;

public class ServiceBusPublisher : IAsyncDisposable
{
    private readonly ServiceBusSender _queueSender;
    private readonly ServiceBusSender _topicSender;

    public ServiceBusPublisher(ServiceBusClient client, IConfiguration configuration)
    {
        var queueName = configuration["ServiceBus:QueueName"] ?? throw new InvalidOperationException("Queue name missing.");
        var topicName = configuration["ServiceBus:TopicName"] ?? throw new InvalidOperationException("Topic name missing.");

        _queueSender = client.CreateSender(queueName);
        _topicSender = client.CreateSender(topicName);
    }

    // Module 2: Queue-based publishing
    public async Task PublishOrderToQueueAsync(OrderCreatedEvent order, CancellationToken cancellationToken = default)
    {
        var message = CreateMessage(order);
        await _queueSender.SendMessageAsync(message, cancellationToken);
    }

    // Module 3: Topic-based publishing
    public async Task PublishOrderToTopicAsync(OrderCreatedEvent order, CancellationToken cancellationToken = default)
    {
        var message = CreateMessage(order);
        await _topicSender.SendMessageAsync(message, cancellationToken);
    }

    private static ServiceBusMessage CreateMessage( OrderCreatedEvent order)
    {
        return new ServiceBusMessage(BinaryData.FromObjectAsJson(order))
        {
            MessageId = order.OrderId.ToString(),
            Subject = "OrderCreated",
            ContentType = "application/json"
        };
    }

    public async ValueTask DisposeAsync()
    {
        await _queueSender.DisposeAsync();
        await _topicSender.DisposeAsync();
    }
}
