
using Azure.Messaging.ServiceBus;
using InventoryConsumer.Worker.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString = builder.Configuration["ServiceBus:ConnectionString"] ?? throw new InvalidOperationException("Service Bus connection string missing.");
    return new ServiceBusClient(connectionString);
});

builder.Services.AddHostedService<InventoryProcessor>();
var host = builder.Build();

host.Run();
