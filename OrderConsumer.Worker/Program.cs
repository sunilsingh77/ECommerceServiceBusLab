
using Azure.Messaging.ServiceBus;
using OrderConsumer.Worker.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString =
        builder.Configuration["ServiceBus:ConnectionString"]
        ?? throw new InvalidOperationException(
            "Service Bus connection string missing.");

    return new ServiceBusClient(connectionString);
});

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options =>
{
    options.IncludeScopes = true;
    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
});

builder.Services.AddHostedService<OrderProcesssor>();

var host = builder.Build();

host.Run();
