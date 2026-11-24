
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

// Load Service Bus connection string from User Secrets.
var configuration = new ConfigurationBuilder()
    .AddUserSecrets<Program>()
    .Build();

var connectionString =  configuration["ServiceBus:ConnectionString"] ?? throw new InvalidOperationException("Service Bus connection string not configured.");

const string queueName = "order-session-queue-v2";

Console.WriteLine("Azure Service Bus Session Lab");
Console.WriteLine("-----------------------------");

await using var client = new ServiceBusClient(connectionString);

await using var sender = client.CreateSender(queueName);

// STEP 1: Create Session Processor.
await using var processor = client.CreateSessionProcessor(queueName,
                                                        new ServiceBusSessionProcessorOptions
                                                        {
                                                            AutoCompleteMessages = false,

                                                            // Process two different order sessions concurrently.
                                                            MaxConcurrentSessions = 2,

                                                            // Process only one message at a time within each session.
                                                            MaxConcurrentCallsPerSession = 1,

                                                            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
                                                        });

// STEP 2: Register message handler.

processor.ProcessMessageAsync += async args =>
{
    var message = args.Message;
    var sessionId = message.SessionId;
    var eventName = message.Body.ToString();

    Console.WriteLine(
        $"START | Session: {sessionId} | Message: {eventName} | " +
        $"DeliveryCount: {message.DeliveryCount}");

    if (eventName == "InventoryReserved")
    {
        Console.WriteLine(
            $"Long-running inventory reservation started: {sessionId}");

        await Task.Delay(
            TimeSpan.FromSeconds(90),
            args.CancellationToken);

        Console.WriteLine(
            $"Long-running inventory reservation completed: {sessionId}");
    }
    else
    {
        await Task.Delay(1500, args.CancellationToken);
    }

    // Simulate one failure for ORDER-101 PaymentCompleted.
    if (sessionId == "ORDER-101" &&
        eventName == "PaymentCompleted" &&
        message.DeliveryCount == 1)
    {
        Console.WriteLine(
            $"FAIL  | Session: {sessionId} | Message: {eventName}");

        await args.AbandonMessageAsync(message);
        return;
    }

    Console.WriteLine(
        $"DONE  | Session: {sessionId} | Message: {eventName}");

    await args.CompleteMessageAsync(message);
};


// STEP 3: Register error handler.
processor.ProcessErrorAsync += args =>
{
    Console.WriteLine($"ERROR | {args.Exception.Message}");
    return Task.CompletedTask;
};

// STEP 4: Start session processing.
await processor.StartProcessingAsync();
Console.WriteLine("Session processor started.");

// STEP 5: Send messages for two orders.
var testMessages = new[]
{
    new { OrderId = "ORDER-101", Event = "OrderCreated" },
    new { OrderId = "ORDER-101", Event = "PaymentCompleted" },
    new { OrderId = "ORDER-101", Event = "InventoryReserved" },

    new { OrderId = "ORDER-102", Event = "OrderCreated" },
    new { OrderId = "ORDER-102", Event = "PaymentCompleted" },
    new { OrderId = "ORDER-102", Event = "InventoryReserved" }
};

foreach (var item in testMessages)
{
    var message = new ServiceBusMessage(BinaryData.FromString(item.Event))
    {
        MessageId = Guid.NewGuid().ToString(),
        // This is the key to session-based ordering.
        SessionId = item.OrderId,
        Subject = item.Event
    };

    await sender.SendMessageAsync(message);

    Console.WriteLine($"SENT | Session: {item.OrderId} | Event: {item.Event}");
}

Console.WriteLine();
Console.WriteLine("Waiting for session processing...");

// Allow enough time for both sessions to finish.
Console.WriteLine("Waiting for long-running session processing...");

// Allow enough time for 90-second operations.
await Task.Delay(TimeSpan.FromSeconds(110));

await processor.StopProcessingAsync();

Console.WriteLine("Session processor stopped.");