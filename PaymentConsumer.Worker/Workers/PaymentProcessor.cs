
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using PaymentConsumer.Worker.Exceptions;
using PaymentConsumer.Worker.Services;

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

            _logger.LogInformation(
                "Processing payment for Order: {OrderId}",
                order.OrderId);

            await using var scope = _scopeFactory.CreateAsyncScope();
            var paymentGateway = scope.ServiceProvider.GetRequiredService<PaymentGatewayService>();

            await paymentGateway.ProcessPaymentAsync(
                order.OrderId,
                order.CustomerName,
                order.TotalAmount,
                args.CancellationToken);

            _logger.LogInformation(
                "Payment successful for Order: {OrderId}",
                order.OrderId);

            if (order.CustomerName.Equals(
                "CRASHONCE",
                StringComparison.OrdinalIgnoreCase)
                && Interlocked.Exchange(ref _crashSimulationTriggered, 1) == 0)
            {
                _logger.LogError(
                    "SIMULATED CRASH: Payment succeeded, but message completion did not happen. OrderId: {OrderId}",
                    order.OrderId);

                throw new Exception(
                    "Simulated consumer crash after successful payment.");
            }

            await args.CompleteMessageAsync(args.Message);
        }
        catch (TransientPaymentException ex)
        {
            var retryCount = args.Message.ApplicationProperties.TryGetValue("RetryCount", out var value)
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

            // 5, 10, 20 seconds
            var delaySeconds = 5 * Math.Pow(2, retryCount);

            var originalMessageId =
                args.Message.ApplicationProperties.TryGetValue("OriginalMessageId", out var originalId)
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
                "Payment retry {RetryCount} scheduled after {Delay} seconds.",
                nextRetry,
                delaySeconds);

            // Complete original only after retry scheduling succeeds.
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
