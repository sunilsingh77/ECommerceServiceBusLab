
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;

namespace NotificationConsumer.Worker.Workers;

public class NotificationProcessor : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _configuration;
    private readonly ILogger<NotificationProcessor> _logger;

    public NotificationProcessor(
        ServiceBusClient client,
        IConfiguration configuration,
        ILogger<NotificationProcessor> logger)
    {
        _client = client;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var topicName = _configuration["ServiceBus:TopicName"]
            ?? throw new InvalidOperationException("Topic name missing.");

        var subscriptionName =
            _configuration["ServiceBus:SubscriptionName"]
            ?? throw new InvalidOperationException("Subscription name missing.");

        await using var processor = _client.CreateProcessor(
            topicName,
            subscriptionName,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1
            });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;

        await processor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation(
            "Notification Consumer started. Listening to {Topic}/{Subscription}",
            topicName,
            subscriptionName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected during application shutdown.
        }
        finally
        {
            await processor.StopProcessingAsync();
        }
    }

    private async Task ProcessMessageAsync(
        ProcessMessageEventArgs args)
    {
        try
        {
            var order = args.Message.Body
                .ToObjectFromJson<OrderCreatedEvent>();

            if (order is null)
                throw new InvalidOperationException("Invalid order message.");

            _logger.LogInformation(
                "Notification Service received Order: {OrderId}",
                order.OrderId);

            _logger.LogInformation(
                "Preparing order confirmation for Customer: {Customer}",
                order.CustomerName);

            // Simulate email delivery.
            await Task.Delay(1000, args.CancellationToken);

            _logger.LogInformation(
                "Email sent successfully to Customer: {Customer}",
                order.CustomerName);

            _logger.LogInformation(
                "Order confirmation completed. OrderId: {OrderId}",
                order.OrderId);

            await args.CompleteMessageAsync(args.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Notification processing failed. MessageId: {MessageId}",
                args.Message.MessageId);

            await args.AbandonMessageAsync(args.Message);
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            "Notification Service Bus error. Entity: {EntityPath}",
            args.EntityPath);

        return Task.CompletedTask;
    }
}
