
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using NotificationConsumer.Worker.Data;
using NotificationConsumer.Worker.Models;
using Microsoft.EntityFrameworkCore;
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


    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        try
        {
            var order = args.Message.Body.ToObjectFromJson<OrderCreatedEvent>();

            if (order is null ||
                order.OrderId == Guid.Empty ||
                string.IsNullOrWhiteSpace(order.CustomerName))
            {
                await args.DeadLetterMessageAsync(
                    args.Message,
                    "InvalidOrder",
                    "Order contains invalid data.");

                return;
            }

            _logger.LogInformation("Notification Service received Order: {OrderId}", order.OrderId);

            await using var scope = _scopeFactory.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<NotificationDbContext>();

            // Preserve original identity across retries and replays.
            var eventId =
                args.Message.ApplicationProperties.TryGetValue(
                    "OriginalMessageId", out var originalId)
                    ? originalId?.ToString() ?? args.Message.MessageId
                    : args.Message.MessageId;

            _logger.LogInformation("Processing notification event. EventId: {EventId}", eventId);

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

                await args.CompleteMessageAsync(args.Message);
                return;
            }

            // STEP 2: Start NotificationDb transaction.
            await using var transaction =
                await dbContext.Database.BeginTransactionAsync(
                    args.CancellationToken);
            try
            {
                _logger.LogInformation(
                    "Creating order confirmation notification for Customer: {Customer}",
                    order.CustomerName);

                // STEP 3: Save durable notification request.
                var notification = new Notification
                {
                    NotificationId = Guid.NewGuid(),
                    OrderId = order.OrderId,
                    CustomerName = order.CustomerName,
                    NotificationType = "OrderConfirmation",
                    NotificationStatus = "Pending",
                    CreatedAt = DateTime.UtcNow
                };

                dbContext.Notifications.Add(notification);

                // STEP 4: Save Inbox record.
                dbContext.InboxMessages.Add(new InboxMessage
                {
                    MessageId = eventId,
                    ProcessedOnUtc = DateTime.UtcNow,
                    ConsumerName = "NotificationConsumer"
                });

                await dbContext.SaveChangesAsync(args.CancellationToken);

                // STEP 5: Commit notification + Inbox together.
                await transaction.CommitAsync(args.CancellationToken);

                _logger.LogInformation(
                    "Notification request and Inbox committed. OrderId: {OrderId}, NotificationId: {NotificationId}",
                    order.OrderId,
                    notification.NotificationId);

                // STEP 6: Complete Service Bus message.
                await args.CompleteMessageAsync(args.Message);
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
                "Notification processing cancelled during shutdown.");
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
