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

    public async Task PublishOrderToQueueAsync(OrderCreatedEvent order, string? correlationId = null, CancellationToken cancellationToken = default)
    {
        var message = CreateMessage(order, correlationId);
        await _queueSender.SendMessageAsync(message, cancellationToken);
    }

    public async Task PublishOrderToTopicAsync(OrderCreatedEvent order, string? correlationId = null, CancellationToken cancellationToken = default)
    {
        var message = CreateMessage(order, correlationId);
        await _topicSender.SendMessageAsync(message, cancellationToken);
    }

    private static ServiceBusMessage CreateMessage(OrderCreatedEvent order, string? correlationId)
    {
        var msg = new ServiceBusMessage(BinaryData.FromObjectAsJson(order))
                    {
                        MessageId = order.OrderId.ToString(),
                        Subject = "OrderCreated",
                        ContentType = "application/json",
                        CorrelationId = string.IsNullOrWhiteSpace(correlationId) ? order.OrderId.ToString() : correlationId
                    };
        return msg;
    }

    public async ValueTask DisposeAsync()
    {
        await _queueSender.DisposeAsync();
        await _topicSender.DisposeAsync();
    }
}