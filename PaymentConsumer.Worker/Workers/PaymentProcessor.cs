
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using PaymentConsumer.Worker.Data;
using PaymentConsumer.Worker.Exceptions;
using PaymentConsumer.Worker.Models;
using PaymentConsumer.Worker.Services;
using Microsoft.EntityFrameworkCore;
namespace PaymentConsumer.Worker.Workers;

public class PaymentProcessor : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _configuration;
    private readonly ILogger<PaymentProcessor> _logger;
    private readonly IServiceScopeFactory _scopeFactory;
    private const int MaxRetries = 3;
    private static int _crashSimulationTriggered;
    public PaymentProcessor(
        ServiceBusClient client,
        IConfiguration configuration,
        ILogger<PaymentProcessor> logger,
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

        var retryQueueName =
            _configuration["ServiceBus:RetryQueueName"]
            ?? throw new InvalidOperationException("Retry queue name missing.");

        await using var retrySender = _client.CreateSender(retryQueueName);

        var options = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(3)
        };

        await using var mainProcessor = _client.CreateProcessor(topicName, subscriptionName, options);
        await using var retryProcessor =  _client.CreateProcessor(retryQueueName, options);

        mainProcessor.ProcessMessageAsync +=  args => ProcessMessageAsync(args, retrySender);
        retryProcessor.ProcessMessageAsync += args => ProcessMessageAsync(args, retrySender);

        mainProcessor.ProcessErrorAsync += ProcessErrorAsync;
        retryProcessor.ProcessErrorAsync += ProcessErrorAsync;

        await mainProcessor.StartProcessingAsync(stoppingToken);
        await retryProcessor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation(
            "Payment Consumer listening to {Topic}/{Subscription} and retry queue {Queue}",
            topicName, subscriptionName, retryQueueName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown.
        }
        finally
        {
            await mainProcessor.StopProcessingAsync();
            await retryProcessor.StopProcessingAsync();
        }
    }


    private async Task ProcessMessageAsync(
        ProcessMessageEventArgs args,
        ServiceBusSender retrySender)
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
                    ["Consumer"] = "PaymentConsumer"
                });

            _logger.LogInformation(
                "Payment message received. CorrelationId: {CorrelationId}, OrderId: {OrderId}",
                correlationId,
                args.Message.MessageId);

            var order = args.Message.Body
                .ToObjectFromJson<OrderCreatedEvent>();

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

            // Resolve PaymentDbContext and PaymentGatewayService
            // from the same DI scope.
            await using var scope = _scopeFactory.CreateAsyncScope();

            var dbContext = scope.ServiceProvider
                .GetRequiredService<PaymentDbContext>();

            var paymentGateway = scope.ServiceProvider
                .GetRequiredService<PaymentGatewayService>();

            // Resolve stable event identity across retries/replays.
            var eventId =
                args.Message.ApplicationProperties.TryGetValue(
                    "OriginalMessageId", out var originalId)
                    ? originalId?.ToString() ?? args.Message.MessageId
                    : args.Message.MessageId;

            _logger.LogInformation(
                "Processing payment for Order: {OrderId}, EventId: {EventId}",
                order.OrderId,
                eventId);

            // STEP 1: Check Inbox.
            var alreadyProcessed = await dbContext.InboxMessages
                .AnyAsync(
                    x => x.MessageId == eventId,
                    args.CancellationToken);

            if (alreadyProcessed)
            {
                _logger.LogWarning(
                    "Duplicate message detected. EventId: {EventId}. Skipping payment.",
                    eventId);

                await args.CompleteMessageAsync(args.Message);
                return;
            }

            // STEP 2: Start SQL transaction.
            await using var transaction =
                await dbContext.Database.BeginTransactionAsync(
                    args.CancellationToken);
            try
            {
                // STEP 3: Process payment.
                // PaymentGatewayService must use this same scoped DbContext.
                await paymentGateway.ProcessPaymentAsync(
                    order.OrderId,
                    order.CustomerName,
                    order.TotalAmount,
                    correlationId,
                    args.CancellationToken);

                _logger.LogInformation(
                    "Payment successful for Order: {OrderId}",
                    order.OrderId);

                // STEP 4: Add Inbox record.
                dbContext.InboxMessages.Add(new InboxMessage
                {
                    MessageId = eventId,
                    ProcessedOnUtc = DateTime.UtcNow,
                    ConsumerName = "PaymentConsumer",
                    CorrelationId = correlationId
                });

                await dbContext.SaveChangesAsync(args.CancellationToken);

                // STEP 5: Commit payment + Inbox together.
                await transaction.CommitAsync(args.CancellationToken);

                _logger.LogInformation(
                    "Payment and Inbox committed. OrderId: {OrderId}, EventId: {EventId}",
                    order.OrderId,
                    eventId);

                // STEP 6: Preserve your existing crash simulation.
                if (order.CustomerName.Equals(
                    "CRASHONCE",
                    StringComparison.OrdinalIgnoreCase)
                    && Interlocked.Exchange(
                        ref _crashSimulationTriggered, 1) == 0)
                {
                    _logger.LogError(
                        "SIMULATED CRASH: Payment and Inbox committed, but message completion did not happen. OrderId: {OrderId}",
                        order.OrderId);

                    throw new Exception(
                        "Simulated consumer crash after successful payment and Inbox commit.");
                }

                // STEP 7: Complete Service Bus message.
                await args.CompleteMessageAsync(args.Message);
            }
            catch
            {
                //await transaction.RollbackAsync(args.CancellationToken);
                throw;
            }
        }
        catch (TransientPaymentException ex)
        {
            var retryCount =
                args.Message.ApplicationProperties.TryGetValue(
                    "RetryCount", out var value)
                    ? Convert.ToInt32(value)
                    : 0;

            _logger.LogWarning(
                ex,
                "Temporary payment failure. RetryCount: {RetryCount}",
                retryCount);

            if (retryCount >= MaxRetries)
            {
                await args.DeadLetterMessageAsync(
                    args.Message,
                    "MaxPaymentRetriesExceeded",
                    ex.Message);

                return;
            }

            var nextRetry = retryCount + 1;

            // 5, 10, 20 seconds.
            var delaySeconds = 5 * Math.Pow(2, retryCount);

            var originalMessageId =
                args.Message.ApplicationProperties.TryGetValue(
                    "OriginalMessageId", out var originalId)
                    ? originalId?.ToString() ?? args.Message.MessageId
                    : args.Message.MessageId;

            var retryMessage = new ServiceBusMessage(args.Message.Body)
            {
                MessageId = $"{originalMessageId}-retry-{nextRetry}",
                CorrelationId = args.Message.CorrelationId,
                Subject = args.Message.Subject,
                ContentType = args.Message.ContentType
            };

            foreach (var property in args.Message.ApplicationProperties)
            {
                retryMessage.ApplicationProperties[property.Key] =
                    property.Value;
            }

            retryMessage.ApplicationProperties["RetryCount"] = nextRetry;
            retryMessage.ApplicationProperties["OriginalMessageId"] =
                originalMessageId;

            var scheduledTime =
                DateTimeOffset.UtcNow.AddSeconds(delaySeconds);

            await retrySender.ScheduleMessageAsync(
                retryMessage,
                scheduledTime,
                args.CancellationToken);

            _logger.LogWarning(
                "Payment retry {RetryCount} scheduled after {Delay} seconds.",
                nextRetry,
                delaySeconds);

            await args.CompleteMessageAsync(args.Message);
        }
        catch (PermanentPaymentException ex)
        {
            _logger.LogError(
                ex,
                "Permanent payment failure. Moving to DLQ.");

            await args.DeadLetterMessageAsync(
                args.Message,
                "PermanentPaymentFailure",
                ex.Message);
        }
        catch (OperationCanceledException)
            when (args.CancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Payment processing cancelled during shutdown.");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Unexpected payment processing error.");

            await args.AbandonMessageAsync(args.Message);
        }
    }


    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            "Payment Service Bus error. Entity: {EntityPath}",
            args.EntityPath);

        return Task.CompletedTask;
    }
}
