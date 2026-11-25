using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using InventoryConsumer.Worker.Data;
using InventoryConsumer.Worker.Models;
using Microsoft.EntityFrameworkCore;

namespace InventoryConsumer.Worker.Workers;

public sealed class InventoryProcessor : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _configuration;
    private readonly ILogger<InventoryProcessor> _logger;
    private readonly IServiceScopeFactory _scopeFactory;

    public InventoryProcessor(
        ServiceBusClient client,
        IConfiguration configuration,
        ILogger<InventoryProcessor> logger,
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
            ?? throw new InvalidOperationException("Service Bus topic name is missing.");

        var subscriptionName = _configuration["ServiceBus:SubscriptionName"]
            ?? throw new InvalidOperationException("Service Bus subscription name is missing.");

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
            "Inventory consumer started. Listening to topic {TopicName}, subscription {SubscriptionName}",
            topicName,
            subscriptionName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Inventory consumer is stopping");
        }
        finally
        {
            await processor.StopProcessingAsync(CancellationToken.None);
            _logger.LogInformation("Inventory Service Bus processor stopped");
        }
    }

    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        var message = args.Message;
        var correlationId = GetCorrelationId(message);

        // Keep the logging scope alive through the catch block too.
        using var logScope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId,
            ["MessageId"] = message.MessageId,
            ["DeliveryCount"] = message.DeliveryCount,
            ["Consumer"] = "InventoryConsumer",
            ["EntityPath"] = args.EntityPath
        });

        try
        {
            _logger.LogInformation(
                "Inventory message received. MessageId: {MessageId}, CorrelationId: {CorrelationId}, DeliveryCount: {DeliveryCount}",
                message.MessageId,
                correlationId,
                message.DeliveryCount);

            var order = message.Body.ToObjectFromJson<OrderCreatedEvent>();

            if (order is null ||
                order.OrderId == Guid.Empty ||
                order.TotalAmount <= 0)
            {
                _logger.LogWarning(
                    "Invalid order event. MessageId: {MessageId}; sending message to dead-letter queue",
                    message.MessageId);

                await args.DeadLetterMessageAsync(
                    message,
                    "InvalidOrder",
                    "Order contains invalid data.",
                    args.CancellationToken);

                return;
            }

            using var orderScope = _logger.BeginScope(
                new Dictionary<string, object?> { ["OrderId"] = order.OrderId });

            _logger.LogInformation(
                "Processing inventory event for OrderId {OrderId}",
                order.OrderId);

            await using var scope = _scopeFactory.CreateAsyncScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

            // Preserve the original event identity when a retry/replay message carries it.
            var eventId = message.ApplicationProperties.TryGetValue(
                "OriginalMessageId",
                out var originalId)
                    ? originalId?.ToString() ?? message.MessageId
                    : message.MessageId;

            _logger.LogDebug("Resolved inventory event identity {EventId}", eventId);

            var alreadyProcessed = await dbContext.InboxMessages
                .AnyAsync(x => x.MessageId == eventId, args.CancellationToken);

            if (alreadyProcessed)
            {
                _logger.LogWarning(
                    "Duplicate inventory event detected for EventId {EventId}; reservation will be skipped",
                    eventId);

                await args.CompleteMessageAsync(message, args.CancellationToken);
                return;
            }

            await using (var transaction =
                await dbContext.Database.BeginTransactionAsync(args.CancellationToken))
            {
                try
                {
                    _logger.LogInformation(
                        "Reserving inventory for customer {CustomerName}, amount {TotalAmount}",
                        order.CustomerName,
                        order.TotalAmount);

                    var reservation = new InventoryReservation
                    {
                        ReservationId = Guid.NewGuid(),
                        OrderId = order.OrderId,
                        CustomerName = order.CustomerName,
                        TotalAmount = order.TotalAmount,
                        ReservationStatus = "Reserved",
                        CreatedAt = DateTime.UtcNow,
                        CorrelationId = correlationId
                    };

                    dbContext.InventoryReservations.Add(reservation);

                    dbContext.InboxMessages.Add(new InboxMessage
                    {
                        MessageId = eventId,
                        ProcessedOnUtc = DateTime.UtcNow,
                        ConsumerName = "InventoryConsumer",
                        CorrelationId = correlationId
                    });

                    await dbContext.SaveChangesAsync(args.CancellationToken);
                    await transaction.CommitAsync(args.CancellationToken);

                    _logger.LogInformation(
                        "Inventory reservation and Inbox record committed for OrderId {OrderId}, EventId {EventId}",
                        order.OrderId,
                        eventId);
                }
                catch
                {
                    await transaction.RollbackAsync(CancellationToken.None);
                    throw;
                }
            }

            // Complete only after the local SQL transaction has committed.
            await args.CompleteMessageAsync(message, args.CancellationToken);

            _logger.LogInformation(
                "Service Bus message completed for OrderId {OrderId}, MessageId {MessageId}",
                order.OrderId,
                message.MessageId);
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Inventory message processing cancelled. MessageId: {MessageId}",
                message.MessageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Inventory message processing failed. MessageId: {MessageId}",
                message.MessageId);

            // Abandon can itself fail if the message lock has expired or settlement
            // has already happened. Log that separately without hiding the original error.
            try
            {
                await args.AbandonMessageAsync(message, cancellationToken: CancellationToken.None);
                _logger.LogWarning(
                    "Inventory message abandoned for redelivery. MessageId: {MessageId}",
                    message.MessageId);
            }
            catch (Exception settlementException)
            {
                _logger.LogError(
                    settlementException,
                    "Could not abandon inventory message. MessageId: {MessageId}",
                    message.MessageId);
            }
        }
    }

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            "Azure Service Bus processor error. EntityPath: {EntityPath}, ErrorSource: {ErrorSource}, Namespace: {FullyQualifiedNamespace}",
            args.EntityPath,
            args.ErrorSource,
            args.FullyQualifiedNamespace);

        return Task.CompletedTask;
    }

    private static string GetCorrelationId(ServiceBusReceivedMessage message)
    {
        if (!string.IsNullOrWhiteSpace(message.CorrelationId))
        {
            return message.CorrelationId;
        }

        if (message.ApplicationProperties.TryGetValue(
                "CorrelationId",
                out var correlationValue) &&
            !string.IsNullOrWhiteSpace(correlationValue?.ToString()))
        {
            return correlationValue!.ToString()!;
        }

        // Fallback ensures every log record has a traceable identifier.
        return message.MessageId;
    }
}
