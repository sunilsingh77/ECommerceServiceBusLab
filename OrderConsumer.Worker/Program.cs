
using Azure.Messaging.ServiceBus;
using OrderConsumer.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString =
        builder.Configuration["ServiceBus:ConnectionString"]
        ?? throw new InvalidOperationException(
            "Service Bus connection string missing.");

    return new ServiceBusClient(connectionString);
});

builder.Services.AddHostedService<Worker>();

var host = builder.Build();

host.Run();
