
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using InventoryConsumer.Worker.Data;
using InventoryConsumer.Worker.Models;
using Microsoft.EntityFrameworkCore;

namespace InventoryConsumer.Worker.Workers;

public class InventoryProcessor : BackgroundService
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
        var topicName = _configuration["ServiceBus:TopicName"] ?? throw new InvalidOperationException("Topic name missing.");
        var subscriptionName = _configuration["ServiceBus:SubscriptionName"] ?? throw new InvalidOperationException("Subscription name missing.");

        await using var processor = _client.CreateProcessor(topicName, subscriptionName,
            new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1
            });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;

        await processor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation(
            "Inventory Consumer started. Listening to {Topic}/{Subscription}",
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

            using var logScope = _logger.BeginScope(
                new Dictionary<string, object?>
                {
                    ["CorrelationId"] = correlationId,
                    ["MessageId"] = args.Message.MessageId,
                    ["Consumer"] = "InventoryConsumer"
                });

            _logger.LogInformation(
                "Inventory message received. CorrelationId: {CorrelationId}, OrderId: {OrderId}",
                correlationId,
                args.Message.MessageId);

            var order = args.Message.Body.ToObjectFromJson<OrderCreatedEvent>();

            if (order is null ||
                order.OrderId == Guid.Empty ||
                order.TotalAmount <= 0)
            {
                await args.DeadLetterMessageAsync(
                    args.Message,
                    "InvalidOrder",
                    "Order contains invalid data.");

                return;
            }

            _logger.LogInformation(
                "Inventory Service received Order: {OrderId}",
                order.OrderId);

            // Resolve InventoryDbContext from a DI scope.
            await using var scope = _scopeFactory.CreateAsyncScope();

            var dbContext = scope.ServiceProvider
                .GetRequiredService<InventoryDbContext>();

            // Use original event identity across retries/replays.
            var eventId =
                args.Message.ApplicationProperties.TryGetValue(
                    "OriginalMessageId", out var originalId)
                    ? originalId?.ToString() ?? args.Message.MessageId
                    : args.Message.MessageId;

            _logger.LogInformation(
                "Processing inventory event. EventId: {EventId}",
                eventId);

            // STEP 1: Check Inbox.
            var alreadyProcessed = await dbContext.InboxMessages
                .AnyAsync(
                    x => x.MessageId == eventId,
                    args.CancellationToken);

            if (alreadyProcessed)
            {
                _logger.LogWarning(
                    "Duplicate inventory event detected. EventId: {EventId}. Skipping reservation.",
                    eventId);

                await args.CompleteMessageAsync(args.Message);
                return;
            }


            // STEP 2: Start local InventoryDb transaction.
            await using var transaction = await dbContext.Database.BeginTransactionAsync(args.CancellationToken);
            try
            {
                _logger.LogInformation(
                    "Reserving inventory for Customer: {Customer}, Amount: {Amount}",
                    order.CustomerName,
                    order.TotalAmount);

                // STEP 3: Perform inventory business operation.
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

                // STEP 4: Record event in Inbox.
                dbContext.InboxMessages.Add(new InboxMessage
                {
                    MessageId = eventId,
                    ProcessedOnUtc = DateTime.UtcNow,
                    ConsumerName = "InventoryConsumer",
                    CorrelationId = correlationId
                });

                await dbContext.SaveChangesAsync(args.CancellationToken);

                // STEP 5: Commit reservation and Inbox together.
                await transaction.CommitAsync(args.CancellationToken);

                _logger.LogInformation(
                    "Inventory reservation and Inbox committed. OrderId: {OrderId}, EventId: {EventId}",
                    order.OrderId,
                    eventId);
            }
            catch
            {
                // Rollback only when database processing fails
                // before the transaction has successfully committed.
                await transaction.RollbackAsync(args.CancellationToken);
                throw;
            }

            // STEP 6: Complete Service Bus message AFTER SQL commit.
            // This must not be inside the SQL transaction catch block.
            await args.CompleteMessageAsync(args.Message);
            _logger.LogInformation("Service Bus message completed. MessageId: {MessageId}", args.Message.MessageId);

        }
        catch (OperationCanceledException)
            when (args.CancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Inventory processing cancelled during shutdown.");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Inventory processing failed. MessageId: {MessageId}",
                args.Message.MessageId);

            await args.AbandonMessageAsync(args.Message);
        }
    }


    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            "Inventory Service Bus error. Entity: {EntityPath}",
            args.EntityPath);

        return Task.CompletedTask;
    }
}
