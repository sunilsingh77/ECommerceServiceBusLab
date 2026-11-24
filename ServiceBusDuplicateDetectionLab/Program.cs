#region Message TTL (Time To Live) and Expiration

using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .AddUserSecrets<Program>()
    .Build();

var connectionString =
    configuration["ServiceBus:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Service Bus connection string missing.");

var queueName =
    configuration["ServiceBus:QueueName"]
    ?? throw new InvalidOperationException(
        "Queue name missing.");

await using var client = new ServiceBusClient(connectionString);
await using var sender = client.CreateSender(queueName);

Console.WriteLine("Service Bus TTL Lab");
Console.WriteLine("-------------------");

// ORDER-301: 30-second TTL
var order301 = new ServiceBusMessage(
    "Payment request - ORDER-301")
{
    MessageId = $"ORDER-301-{Guid.NewGuid()}",
    Subject = "PaymentProcessing",
    TimeToLive = TimeSpan.FromSeconds(30)
};

await sender.SendMessageAsync(order301);

Console.WriteLine("SENT | ORDER-301 | TTL: 30 seconds");

// ORDER-302: 5-second TTL
var order302 = new ServiceBusMessage(
    "Payment request - ORDER-302")
{
    MessageId = $"ORDER-302-{Guid.NewGuid()}",
    Subject = "PaymentProcessing",
    TimeToLive = TimeSpan.FromSeconds(5)
};

await sender.SendMessageAsync(order302);

Console.WriteLine("SENT | ORDER-302 | TTL: 5 seconds");

Console.WriteLine("Waiting 10 seconds...");
await Task.Delay(TimeSpan.FromSeconds(10));

Console.WriteLine("10 seconds elapsed.");
Console.WriteLine("Check the queue using Azure Service Bus Explorer.");

#endregion

#region Session based queue handling messages
//////////////////////////////////////////////////
///Session based queue handling messages
//////////////////////////////////////////////////
/*
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .AddUserSecrets<Program>()
    .Build();

var connectionString =
    configuration["ServiceBus:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Service Bus connection string missing.");

var queueName =
    configuration["ServiceBus:QueueName"]
    ?? throw new InvalidOperationException(
        "Queue name missing.");

await using var client = new ServiceBusClient(connectionString);
await using var sender = client.CreateSender(queueName);

Console.WriteLine("Service Bus Scheduling Lab");
Console.WriteLine("--------------------------");

// 1. Schedule ORDER-201 for delivery after 30 seconds.
var order201 = new ServiceBusMessage("Order payment processing - ORDER-201")
{
    MessageId = $"ORDER-201-{Guid.NewGuid()}",
    Subject = "PaymentProcessing",
    ContentType = "text/plain"
};

var scheduledTime201 = DateTimeOffset.UtcNow.AddSeconds(30);
long sequenceNumber201 = await sender.ScheduleMessageAsync(order201, scheduledTime201);

Console.WriteLine($"SCHEDULED | ORDER-201 | Sequence: {sequenceNumber201}");
Console.WriteLine($"Scheduled delivery: {scheduledTime201:O}");

// 2. Schedule ORDER-202 for delivery after 60 seconds.
var order202 = new ServiceBusMessage("Order payment processing - ORDER-202")
{
    MessageId = $"ORDER-202-{Guid.NewGuid()}",
    Subject = "PaymentProcessing",
    ContentType = "text/plain"
};

var scheduledTime202 = DateTimeOffset.UtcNow.AddSeconds(60);
long sequenceNumber202 = await sender.ScheduleMessageAsync(order202, scheduledTime202);
Console.WriteLine($"SCHEDULED | ORDER-202 | Sequence: {sequenceNumber202}");

// 3. Cancel ORDER-202 before its scheduled delivery.
await Task.Delay(TimeSpan.FromSeconds(5));
await sender.CancelScheduledMessageAsync(sequenceNumber202);

Console.WriteLine($"CANCELLED | ORDER-202 | Sequence: {sequenceNumber202}");
Console.WriteLine("Scheduling test completed.");
*/
#endregion

#region for duplicate detection, we need to send messages with the same MessageId
// Same MessageId for all three send attempts.
//var messageId = $"ORDER-101-{Guid.NewGuid()}";

//for (int i = 1; i <= 3; i++)
//{
//    var message = new ServiceBusMessage("OrderCreated event - Same OrderId: ORDER-101")
//    {
//        MessageId = Guid.NewGuid().ToString(),
//        Subject = "OrderCreated",
//        ContentType = "text/plain"
//    };

//    message.ApplicationProperties["OrderId"] = "ORDER-101";
//    await sender.SendMessageAsync(message);

//    Console.WriteLine($"SEND ATTEMPT {i} | MessageId: {message.MessageId} | OrderId: ORDER-101");
//}
#endregion