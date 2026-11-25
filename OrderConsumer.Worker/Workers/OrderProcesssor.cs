
using Azure.Messaging.ServiceBus;
using ECommerce.Contracts;
using System.Text.Json;

namespace OrderConsumer.Worker.Workers;

public class OrderProcesssor : BackgroundService
{
    private readonly ServiceBusClient _client;
    private readonly IConfiguration _configuration;
    private readonly ILogger<OrderProcesssor> _logger;

    public OrderProcesssor(ServiceBusClient client, IConfiguration configuration, ILogger<OrderProcesssor> logger)
    {
        _client = client;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueName = _configuration["ServiceBus:QueueName"] ?? throw new InvalidOperationException("Queue name missing.");
        await using var processor = _client.CreateProcessor(queueName, 
            new ServiceBusProcessorOptions { 
                AutoCompleteMessages = false, 
                MaxConcurrentCalls = 1, 
                // Maximum period for automatic lock renewal.
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(3)
        });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;

        await processor.StartProcessingAsync(stoppingToken);
        _logger.LogInformation("Order Consumer started. Listening to {QueueName}", queueName);

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected when the worker is shutting down.
        }
        finally
        {
            await processor.StopProcessingAsync();
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

        using var logScope = _logger.BeginScope(
                                                new Dictionary<string, object?>
                                                {
                                                    ["CorrelationId"] = correlationId,
                                                    ["MessageId"] = args.Message.MessageId,
                                                    ["DeliveryCount"] = args.Message.DeliveryCount,
                                                    ["Consumer"] = "OrderConsumer"
                                                });
        try
        {
            var order = args.Message.Body.ToObjectFromJson<OrderCreatedEvent>();

            _logger.LogInformation(
                "Order message received. CorrelationId: {CorrelationId}, OrderId: {OrderId}",
                correlationId,
                order?.OrderId);

            // Scenario 1: Invalid message - permanent failure
            if (order is null || order.OrderId == Guid.Empty || string.IsNullOrWhiteSpace(order.CustomerName) || order.TotalAmount <= 0)
            {
                _logger.LogWarning(
                    "Invalid order message. Moving to DLQ. MessageId: {MessageId}",
                    args.Message.MessageId);

                await args.DeadLetterMessageAsync(
                    args.Message,
                    "InvalidOrder",
                    "Order contains invalid data.");

                return;
            }

            _logger.LogInformation(
                "Processing Order: {OrderId}, Customer: {Customer}, Amount: {Amount}, DeliveryCount: {DeliveryCount}, CorrelationId: {CorrelationId}",
                order.OrderId,
                order.CustomerName,
                order.TotalAmount,
                args.Message.DeliveryCount,
                correlationId);

            // Scenario 2: Simulate temporary processing failure
            if (order.CustomerName.Equals("FAIL", StringComparison.OrdinalIgnoreCase))
            {
                throw new Exception("Simulated temporary processing failure.");
            }
            if (order.CustomerName.Equals("DEFER", StringComparison.OrdinalIgnoreCase))
            {
                await DeferAndRetrieveAsync(args);
                //return;
            }
            // Simulate business processing
            await Task.Delay(1000, args.CancellationToken);

            _logger.LogInformation(
                "Order processed successfully. OrderId: {OrderId}, CorrelationId: {CorrelationId}",
                order.OrderId,
                correlationId);

            // Scenario 3: Successful processing
            await args.CompleteMessageAsync(args.Message);

            _logger.LogInformation(
                "Message completed successfully. MessageId: {MessageId}, CorrelationId: {CorrelationId}",
                args.Message.MessageId,
                correlationId);
        }
        catch (OperationCanceledException)
            when (args.CancellationToken.IsCancellationRequested)
        {
            // Application is shutting down.
            // Do not settle the message; the lock will eventually expire.
            _logger.LogInformation("Message processing cancelled during shutdown.");
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Order processing failed. MessageId: {MessageId}, DeliveryCount: {DeliveryCount}",
                args.Message.MessageId,
                args.Message.DeliveryCount);

            // Scenario 4: Temporary failure - release for redelivery
            await args.AbandonMessageAsync(args.Message);
        }
    }
    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        _logger.LogError(
            args.Exception,
            """
            Service Bus processing error.
            EntityPath: {EntityPath}
            ErrorSource: {ErrorSource}
            FullyQualifiedNamespace: {Namespace}
            """,
            args.EntityPath,
            args.ErrorSource,
            args.FullyQualifiedNamespace);

        return Task.CompletedTask;
    }
    private async Task DeferAndRetrieveAsync(
        ProcessMessageEventArgs args)
    {
        var sequenceNumber = args.Message.SequenceNumber;

        _logger.LogInformation(
            "Deferring message. SequenceNumber: {SequenceNumber}",
            sequenceNumber);

        // Release the message into the deferred state.
        await args.DeferMessageAsync(args.Message);

        // Simulate waiting for a business dependency.
        _logger.LogInformation(
            "Waiting for payment confirmation...");

        await Task.Delay(
            TimeSpan.FromSeconds(10),
            args.CancellationToken);

        var queueName = _configuration["ServiceBus:QueueName"]
            ?? throw new InvalidOperationException("Queue name missing.");

        await using var receiver = _client.CreateReceiver(queueName);

        // Retrieve the exact deferred message.
        var deferredMessage =
            await receiver.ReceiveDeferredMessageAsync(
                sequenceNumber,
                args.CancellationToken);

        if (deferredMessage is null)
        {
            _logger.LogWarning(
                "Deferred message not found: {SequenceNumber}",
                sequenceNumber);

            return;
        }

        try
        {
            var order = deferredMessage.Body
                .ToObjectFromJson<OrderCreatedEvent>();

            if (order is null)
                throw new InvalidOperationException("Invalid deferred order.");

            _logger.LogInformation(
                "Deferred Order retrieved: {OrderId}",
                order.OrderId);

            // Simulate processing after dependency is available.
            await Task.Delay(1000, args.CancellationToken);

            await receiver.CompleteMessageAsync(
                deferredMessage,
                args.CancellationToken);

            _logger.LogInformation(
                "Deferred Order processed successfully: {OrderId}",
                order.OrderId);
        }
        catch
        {
            // Return the deferred message to its deferred state.
            await receiver.DeferMessageAsync(
                deferredMessage,
                cancellationToken: args.CancellationToken);

            throw;
        }
    }

}
