using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using Microsoft.EntityFrameworkCore;
using PaymentConsumer.Worker.Data;
using PaymentConsumer.Worker.Exceptions;
using PaymentConsumer.Worker.Models;
using PaymentConsumer.Worker.Services;

namespace PaymentConsumer.Worker.Workers;

public sealed class PaymentProcessor : BackgroundService
{
    private const int MaxRetries = 3;

    private readonly ServiceBusClient _client;
    private readonly IConfiguration _configuration;
    private readonly ILogger<PaymentProcessor> _logger;
    private readonly IServiceScopeFactory _scopeFactory;

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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var topicName = _configuration["ServiceBus:TopicName"]
            ?? throw new InvalidOperationException("Service Bus topic name is missing.");

        var subscriptionName = _configuration["ServiceBus:SubscriptionName"]
            ?? throw new InvalidOperationException("Service Bus subscription name is missing.");

        var retryQueueName = _configuration["ServiceBus:RetryQueueName"]
            ?? throw new InvalidOperationException("Service Bus retry queue name is missing.");

        await using var retrySender = _client.CreateSender(retryQueueName);

        var processorOptions = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(3)
        };

        await using var mainProcessor =
            _client.CreateProcessor(topicName, subscriptionName, processorOptions);

        await using var retryProcessor =
            _client.CreateProcessor(retryQueueName, processorOptions);

        mainProcessor.ProcessMessageAsync += args =>
            ProcessMessageAsync(args, retrySender);

        retryProcessor.ProcessMessageAsync += args =>
            ProcessMessageAsync(args, retrySender);

        mainProcessor.ProcessErrorAsync += ProcessErrorAsync;
        retryProcessor.ProcessErrorAsync += ProcessErrorAsync;

        await mainProcessor.StartProcessingAsync(stoppingToken);
        await retryProcessor.StartProcessingAsync(stoppingToken);

        _logger.LogInformation(
            "Payment consumer started. Listening to topic {TopicName}, subscription {SubscriptionName}, and retry queue {RetryQueueName}",
            topicName,
            subscriptionName,
            retryQueueName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Payment consumer is stopping");
        }
        finally
        {
            await mainProcessor.StopProcessingAsync(CancellationToken.None);
            await retryProcessor.StopProcessingAsync(CancellationToken.None);
            _logger.LogInformation("Payment Service Bus processors stopped");
        }
    }

    private async Task ProcessMessageAsync(
        ProcessMessageEventArgs args,
        ServiceBusSender retrySender)
    {
        var message = args.Message;
        var correlationId = GetCorrelationId(message);

        // Scope is deliberately created before try so it is present in
        // success, retry, DLQ, cancellation, and exception log records.
        using var logScope = _logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId,
            ["MessageId"] = message.MessageId,
            ["DeliveryCount"] = message.DeliveryCount,
            ["Consumer"] = "PaymentConsumer",
            ["EntityPath"] = args.EntityPath
        });

        try
        {
            _logger.LogInformation(
                "Payment message received. MessageId: {MessageId}, CorrelationId: {CorrelationId}, DeliveryCount: {DeliveryCount}",
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

            await ProcessValidOrderAsync(args, retrySender, order, correlationId);
        }
        catch (TransientPaymentException ex)
        {
            await HandleTransientFailureAsync(args, retrySender, ex, correlationId);
        }
        catch (PermanentPaymentException ex)
        {
            _logger.LogError(
                ex,
                "Permanent payment failure. Moving message to DLQ. MessageId: {MessageId}",
                message.MessageId);

            await args.DeadLetterMessageAsync(
                message,
                "PermanentPaymentFailure",
                ex.Message,
                args.CancellationToken);
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Payment processing cancelled. MessageId: {MessageId}",
                message.MessageId);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Unexpected payment processing error. MessageId: {MessageId}",
                message.MessageId);

            try
            {
                await args.AbandonMessageAsync(
                    message,
                    cancellationToken: CancellationToken.None);

                _logger.LogWarning(
                    "Payment message abandoned for redelivery. MessageId: {MessageId}",
                    message.MessageId);
            }
            catch (Exception settlementException)
            {
                _logger.LogError(
                    settlementException,
                    "Could not abandon payment message. MessageId: {MessageId}",
                    message.MessageId);
            }
        }
    }

    private async Task ProcessValidOrderAsync(
        ProcessMessageEventArgs args,
        ServiceBusSender retrySender,
        OrderCreatedEvent order,
        string correlationId)
    {
        var message = args.Message;

        _logger.LogInformation(
            "Processing payment for OrderId {OrderId}",
            order.OrderId);

        // PaymentDbContext and PaymentGatewayService are resolved from
        // the same DI scope so both use the same local SQL transaction.
        await using var scope = _scopeFactory.CreateAsyncScope();

        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentDbContext>();
        var paymentGateway = scope.ServiceProvider.GetRequiredService<PaymentGatewayService>();

        var eventId = GetOriginalEventId(message);

        _logger.LogDebug("Resolved payment event identity {EventId}", eventId);

        var alreadyProcessed = await dbContext.InboxMessages
            .AnyAsync(x => x.MessageId == eventId, args.CancellationToken);

        if (alreadyProcessed)
        {
            _logger.LogWarning(
                "Duplicate payment event detected. EventId: {EventId}; skipping payment",
                eventId);

            await args.CompleteMessageAsync(message, args.CancellationToken);
            return;
        }

        // PaymentTransactions and InboxMessages are committed atomically
        // in the Payment service's own database.
        await using (var transaction =
            await dbContext.Database.BeginTransactionAsync(args.CancellationToken))
        {
            try
            {
                await paymentGateway.ProcessPaymentAsync(
                    order.OrderId,
                    order.CustomerName,
                    order.TotalAmount,
                    correlationId,
                    args.CancellationToken);

                _logger.LogInformation(
                    "Payment operation completed for OrderId {OrderId}",
                    order.OrderId);

                dbContext.InboxMessages.Add(new InboxMessage
                {
                    MessageId = eventId,
                    ProcessedOnUtc = DateTime.UtcNow,
                    ConsumerName = "PaymentConsumer",
                    CorrelationId = correlationId
                });

                await dbContext.SaveChangesAsync(args.CancellationToken);
                await transaction.CommitAsync(args.CancellationToken);

                _logger.LogInformation(
                    "Payment and Inbox transaction committed. OrderId: {OrderId}, EventId: {EventId}",
                    order.OrderId,
                    eventId);
            }
            catch
            {
                // Disposing an uncommitted EF Core transaction rolls it back.
                // Use a non-cancelled token so rollback can still be attempted
                // when the message cancellation token has been signalled.
                await transaction.RollbackAsync(CancellationToken.None);
                throw;
            }
        }

        // Preserve the existing crash-after-commit test scenario.
        if (order.CustomerName.Equals(
                "CRASHONCE",
                StringComparison.OrdinalIgnoreCase) &&
            Interlocked.Exchange(ref _crashSimulationTriggered, 1) == 0)
        {
            _logger.LogError(
                "SIMULATED CRASH: Payment and Inbox committed, but message completion did not happen. OrderId: {OrderId}",
                order.OrderId);

            throw new Exception(
                "Simulated consumer crash after successful payment and Inbox commit.");
        }

        // Complete only after the database transaction commits.
        await args.CompleteMessageAsync(message, args.CancellationToken);

        _logger.LogInformation(
            "Payment Service Bus message completed. OrderId: {OrderId}, MessageId: {MessageId}",
            order.OrderId,
            message.MessageId);
    }

    private async Task HandleTransientFailureAsync(
        ProcessMessageEventArgs args,
        ServiceBusSender retrySender,
        TransientPaymentException exception,
        string correlationId)
    {
        var message = args.Message;
        var retryCount = GetRetryCount(message);

        _logger.LogWarning(
            exception,
            "Temporary payment failure. RetryCount: {RetryCount}, MaxRetries: {MaxRetries}",
            retryCount,
            MaxRetries);

        if (retryCount >= MaxRetries)
        {
            _logger.LogError(
                "Payment retry limit reached. Sending message to DLQ. RetryCount: {RetryCount}",
                retryCount);

            await args.DeadLetterMessageAsync(
                message,
                "MaxPaymentRetriesExceeded",
                exception.Message,
                args.CancellationToken);

            return;
        }

        var nextRetry = retryCount + 1;
        var delaySeconds = 5 * Math.Pow(2, retryCount);
        var originalMessageId = GetOriginalEventId(message);

        var retryMessage = new ServiceBusMessage(message.Body)
        {
            MessageId = $"{originalMessageId}-retry-{nextRetry}",
            CorrelationId = correlationId,
            Subject = message.Subject,
            ContentType = message.ContentType
        };

        foreach (var property in message.ApplicationProperties)
        {
            retryMessage.ApplicationProperties[property.Key] = property.Value;
        }

        retryMessage.ApplicationProperties["RetryCount"] = nextRetry;
        retryMessage.ApplicationProperties["OriginalMessageId"] = originalMessageId;

        var scheduledTime = DateTimeOffset.UtcNow.AddSeconds(delaySeconds);

        await retrySender.ScheduleMessageAsync(
            retryMessage,
            scheduledTime,
            args.CancellationToken);

        _logger.LogWarning(
            "Payment retry scheduled. RetryCount: {RetryCount}, DelaySeconds: {DelaySeconds}, ScheduledTimeUtc: {ScheduledTimeUtc}, RetryMessageId: {RetryMessageId}",
            nextRetry,
            delaySeconds,
            scheduledTime,
            retryMessage.MessageId);

        // Complete the current message only after retry scheduling succeeds.
        await args.CompleteMessageAsync(message, args.CancellationToken);

        _logger.LogInformation(
            "Original payment message completed after retry scheduling. MessageId: {MessageId}",
            message.MessageId);
    }

    private static int GetRetryCount(ServiceBusReceivedMessage message)
    {
        if (!message.ApplicationProperties.TryGetValue("RetryCount", out var value) ||
            value is null)
        {
            return 0;
        }

        return Convert.ToInt32(value);
    }

    private static string GetOriginalEventId(ServiceBusReceivedMessage message)
    {
        if (message.ApplicationProperties.TryGetValue(
                "OriginalMessageId",
                out var originalId) &&
            !string.IsNullOrWhiteSpace(originalId?.ToString()))
        {
            return originalId!.ToString()!;
        }

        return message.MessageId;
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

        return message.MessageId;
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
}
