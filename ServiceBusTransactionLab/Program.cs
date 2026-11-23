using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using System.Transactions;

Console.WriteLine("Azure Service Bus Transaction Lab");
Console.WriteLine("----------------------------------");

var configuration = new ConfigurationBuilder()
    .AddUserSecrets<Program>()
    .Build();

var connectionString = configuration["ServiceBus:ConnectionString"] ?? throw new InvalidOperationException("Connection string missing.");
//var connectionString = "";


var sourceQueue = "transaction-source-queue";//configuration["ServiceBus:SourceQueue"] ?? throw new InvalidOperationException("Source Queue missing.");
var targetQueue = "transaction-target-queue";//configuration["ServiceBus:TargetQueue"] ?? throw new InvalidOperationException("Target Queue missing.");

var clientOptions = new ServiceBusClientOptions
{
    EnableCrossEntityTransactions = true
};

await using var client = new ServiceBusClient(connectionString, clientOptions);

await using var sourceSender = client.CreateSender(sourceQueue);
await using var targetSender = client.CreateSender(targetQueue);

await using var receiver = client.CreateReceiver(sourceQueue,
                                                    new ServiceBusReceiverOptions
                                                    {
                                                        ReceiveMode = ServiceBusReceiveMode.PeekLock
                                                    });

// STEP 1: Send a test message to source queue.
var testMessage = new ServiceBusMessage("Order transaction test message")
                                                    {
                                                        MessageId = Guid.NewGuid().ToString(),
                                                        Subject = "TransactionTest"
                                                    };

await sourceSender.SendMessageAsync(testMessage);

Console.WriteLine("Test message sent to source queue.");

// STEP 2: Receive original message.
var receivedMessage = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));

if (receivedMessage is null)
{
    Console.WriteLine("No message received.");
    return;
}

Console.WriteLine($"Received message: {receivedMessage.MessageId}");

// STEP 3: Start transaction.
try
{
    using (var transactionScope = new TransactionScope(
        TransactionScopeOption.Required,
        new TransactionOptions
        {
            Timeout = TimeSpan.FromSeconds(60)
        },
        TransactionScopeAsyncFlowOption.Enabled))
    {
        var forwardedMessage = new ServiceBusMessage(
            receivedMessage.Body)
        {
            MessageId = receivedMessage.MessageId
        };

        await targetSender.SendMessageAsync(forwardedMessage);

        await receiver.CompleteMessageAsync(receivedMessage);

        Console.WriteLine("Both operations requested.");

        throw new Exception(
            "Simulated failure before transaction commit.");
        //transactionScope.Complete();
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Transaction rolled back: {ex.Message}");
}

Console.WriteLine("Transaction experiment completed.");
