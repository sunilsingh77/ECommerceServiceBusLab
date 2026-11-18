
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .AddUserSecrets<Program>()
    .Build();

var connectionString = configuration["ServiceBus:ConnectionString"] ?? throw new InvalidOperationException("Connection string missing.");
//var topicName = configuration["ServiceBus:TopicName"] ?? throw new InvalidOperationException("Topic name missing.");
//var subscriptionName = configuration["ServiceBus:SubscriptionName"] ?? throw new InvalidOperationException("Subscription name missing.");
var retryQueueName = configuration["ServiceBus:RetryQueueName"] ?? throw new InvalidOperationException("Retry queue name missing.");

await using var client = new ServiceBusClient(connectionString);

// Connect specifically to the subscription Dead Letter Queue.
//await using var dlqReceiver = client.CreateReceiver(
//    topicName,
//    subscriptionName,
//    new ServiceBusReceiverOptions
//    {
//        SubQueue = SubQueue.DeadLetter,
//        ReceiveMode = ServiceBusReceiveMode.PeekLock
//    });
// Connect to payment-retry-queue Dead Letter Queue.
await using var dlqReceiver = client.CreateReceiver(
    retryQueueName,
    new ServiceBusReceiverOptions
    {
        SubQueue = SubQueue.DeadLetter,
        ReceiveMode = ServiceBusReceiveMode.PeekLock
    });
await using var retrySender = client.CreateSender(retryQueueName);

Console.WriteLine("Payment DLQ Recovery Tool");
Console.WriteLine("-------------------------");

var messages = await dlqReceiver.ReceiveMessagesAsync(
    maxMessages: 10,
    maxWaitTime: TimeSpan.FromSeconds(5));

if (messages.Count == 0)
{
    Console.WriteLine("No messages found in Payment DLQ.");
    return;
}

foreach (var message in messages)
{
    Console.WriteLine();
    Console.WriteLine($"Message ID: {message.MessageId}");
    Console.WriteLine($"Dead Letter Reason: {message.DeadLetterReason}");
    Console.WriteLine($"Description: {message.DeadLetterErrorDescription}");
    Console.WriteLine($"Delivery Count: {message.DeliveryCount}");

    // Only replay messages that exhausted our transient retry policy.
    if (message.DeadLetterReason != "MaxPaymentRetriesExceeded")
    {
        Console.WriteLine("Skipping: This message is not eligible for automatic replay.");

        await dlqReceiver.AbandonMessageAsync(message);
        continue;
    }

    Console.Write("Replay this message? (Y/N): ");

    var answer = Console.ReadLine();

    if (!string.Equals(answer, "Y", StringComparison.OrdinalIgnoreCase))
    {
        await dlqReceiver.AbandonMessageAsync(message);
        Console.WriteLine("Message left in DLQ.");
        continue;
    }

    var originalMessageId =
        message.ApplicationProperties.TryGetValue(
            "OriginalMessageId", out var originalId)
            ? originalId?.ToString() ?? message.MessageId
            : message.MessageId;

    var replayMessage = new ServiceBusMessage(message.Body)
    {
        MessageId = $"{originalMessageId}-replay-{Guid.NewGuid():N}",
        CorrelationId = message.CorrelationId,
        Subject = message.Subject,
        ContentType = message.ContentType
    };

    // Preserve useful business metadata but reset retry count.
    foreach (var property in message.ApplicationProperties)
    {
        replayMessage.ApplicationProperties[property.Key] = property.Value;
    }

    replayMessage.ApplicationProperties["OriginalMessageId"] =
        originalMessageId;

    replayMessage.ApplicationProperties["RetryCount"] = 0;
    replayMessage.ApplicationProperties["IsReplay"] = true;

    try
    {
        // Renew the lock before attempting recovery.
        await dlqReceiver.RenewMessageLockAsync(message);

        // Send the replay message.
        await retrySender.SendMessageAsync(replayMessage);

        Console.WriteLine("Replay message sent successfully.");

        // Complete original DLQ message.
        await dlqReceiver.CompleteMessageAsync(message);

        Console.WriteLine("Original DLQ message completed successfully.");    
    }
    catch (ServiceBusException ex)
    {
        Console.WriteLine($"Service Bus error: {ex.Reason} - {ex.Message}");

        // Do not assume the replay send failed just because
        // completing the original message failed.
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Replay failed: {ex.Message}");
    }
}

Console.WriteLine();
Console.WriteLine("DLQ recovery execution completed.");
