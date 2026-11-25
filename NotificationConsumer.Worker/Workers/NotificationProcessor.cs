using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using Microsoft.EntityFrameworkCore;
using NotificationConsumer.Worker.Data;
using NotificationConsumer.Worker.Models;

namespace NotificationConsumer.Worker.Workers;

public class NotificationProcessor : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _configuration;
    private readonly ILogger<NotificationProcessor> _logger;
    private readonly IServiceScopeFactory _scopeFactory;

    public NotificationProcessor(
        ServiceBusClient client,
        IConfiguration configuration,
        ILogger<NotificationProcessor> logger,
        IServiceScopeFactory scopeFactory)
    {
        _client = client;
        _configuration = configuration;
        _logger = logger;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topicName = _configuration["ServiceBus:TopicName"]
            ?? throw new InvalidOperationException("Topic name missing.");

        var subscriptionName = _configuration["ServiceBus:SubscriptionName"]
            ?? throw new InvalidOperationException("Subscription name missing.");

        await using var processor = _client.CreateProcessor(
            topicName,
            subscriptionName,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1,
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(3)
            });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;

        await processor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation(
            "Notification consumer started. Listening to topic {TopicName}, subscription {SubscriptionName}",
            topicName,
            subscriptionName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Notification consumer shutdown requested");
        }
        finally
        {
            await processor.StopProcessingAsync();
            _logger.LogInformation("Notification consumer stopped");
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        var correlationId = args.Message.CorrelationId;

        if (string.IsNullOrWhiteSpace(correlationId) &&
            args.Message.ApplicationProperties.TryGetValue(
                "CorrelationId", out var correlationValue))
        {
            correlationId = correlationValue?.ToString();
        }

        if (string.IsNullOrWhiteSpace(correlationId))
        {
            correlationId = args.Message.MessageId;
        }

        using var logScope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId,
            ["MessageId"] = args.Message.MessageId,
            ["DeliveryCount"] = args.Message.DeliveryCount,
            ["Consumer"] = "NotificationConsumer",
            ["EntityPath"] = args.EntityPath
        });

        try
        {
            _logger.LogInformation(
                "Notification message received. MessageId: {MessageId}, CorrelationId: {CorrelationId}, DeliveryCount: {DeliveryCount}",
                args.Message.MessageId,
                correlationId,
                args.Message.DeliveryCount);

            var order = args.Message.Body.ToObjectFromJson<OrderCreatedEvent>();

            if (order is null ||
                order.OrderId == Guid.Empty ||
                string.IsNullOrWhiteSpace(order.CustomerName))
            {
                _logger.LogWarning(
                    "Invalid order event received. MessageId: {MessageId}. Dead-lettering message.",
                    args.Message.MessageId);

                await args.DeadLetterMessageAsync(
                    args.Message,
                    "InvalidOrder",
                    "Order contains invalid data.",
                    args.CancellationToken);

                return;
            }

            using var orderScope = _logger.BeginScope(
                new Dictionary<string, object?> { ["OrderId"] = order.OrderId });

            _logger.LogInformation(
                "Processing notification for OrderId {OrderId}",
                order.OrderId);

            await using var scope = _scopeFactory.CreateAsyncScope();
            var dbContext = scope.ServiceProvider
                .GetRequiredService<NotificationDbContext>();

            // Preserve the original event identity across retries and replays.
            var eventId =
                args.Message.ApplicationProperties.TryGetValue(
                    "OriginalMessageId", out var originalId)
                    ? originalId?.ToString() ?? args.Message.MessageId
                    : args.Message.MessageId;

            _logger.LogInformation(
                "Checking notification Inbox. EventId: {EventId}",
                eventId);

            // STEP 1: Check Inbox.
            var alreadyProcessed = await dbContext.InboxMessages
                .AnyAsync(
                    x => x.MessageId == eventId,
                    args.CancellationToken);

            if (alreadyProcessed)
            {
                _logger.LogWarning(
                    "Duplicate notification event detected. EventId: {EventId}. Skipping notification creation.",
                    eventId);

                await args.CompleteMessageAsync(args.Message, args.CancellationToken);
                _logger.LogInformation(
                    "Duplicate notification message completed. EventId: {EventId}",
                    eventId);
                return;
            }

            // STEP 2: Start NotificationDb transaction.
            await using var transaction =
                await dbContext.Database.BeginTransactionAsync(
                    args.CancellationToken);

            try
            {
                _logger.LogInformation(
                    "Creating order confirmation notification for customer {CustomerName}",
                    order.CustomerName);

                // STEP 3: Save durable notification request.
                var notification = new Notification
                {
                    NotificationId = Guid.NewGuid(),
                    OrderId = order.OrderId,
                    CustomerName = order.CustomerName,
                    NotificationType = "OrderConfirmation",
                    NotificationStatus = "Pending",
                    CreatedAt = DateTime.UtcNow,
                    CorrelationId = correlationId
                };

                dbContext.Notifications.Add(notification);

                // STEP 4: Save Inbox record.
                dbContext.InboxMessages.Add(new InboxMessage
                {
                    MessageId = eventId,
                    ProcessedOnUtc = DateTime.UtcNow,
                    ConsumerName = "NotificationConsumer",
                    CorrelationId = correlationId
                });

                await dbContext.SaveChangesAsync(args.CancellationToken);

                // STEP 5: Commit notification + Inbox together.
                await transaction.CommitAsync(args.CancellationToken);

                _logger.LogInformation(
                    "Notification request and Inbox committed. OrderId: {OrderId}, NotificationId: {NotificationId}, EventId: {EventId}",
                    order.OrderId,
                    notification.NotificationId,
                    eventId);

                // STEP 6: Complete Service Bus message.
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);

                _logger.LogInformation(
                    "Notification Service Bus message completed. OrderId: {OrderId}, MessageId: {MessageId}",
                    order.OrderId,
                    args.Message.MessageId);
            }
            catch
            {
                await transaction.RollbackAsync(args.CancellationToken);
                throw;
            }
        }
        catch (OperationCanceledException)
            when (args.CancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Notification processing cancelled during shutdown");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Notification processing failed. MessageId: {MessageId}",
                args.Message.MessageId);

            try
            {
                await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
                _logger.LogWarning(
                    "Notification message abandoned for retry. MessageId: {MessageId}",
                    args.Message.MessageId);
            }
            catch (Exception settlementException)
            {
                _logger.LogError(
                    settlementException,
                    "Failed to abandon notification message. MessageId: {MessageId}",
                    args.Message.MessageId);
            }
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            "Notification Service Bus processor error. EntityPath: {EntityPath}, ErrorSource: {ErrorSource}, Namespace: {FullyQualifiedNamespace}",
            args.EntityPath,
            args.ErrorSource,
            args.FullyQualifiedNamespace);

        return Task.CompletedTask;
    }
}
