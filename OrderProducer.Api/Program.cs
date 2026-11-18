
using Azure.Messaging.ServiceBus;
using OrderProducer.Api.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString =
        builder.Configuration["ServiceBus:ConnectionString"]
        ?? throw new InvalidOperationException(
            "Service Bus connection string missing.");

    return new ServiceBusClient(connectionString);
});

builder.Services.AddSingleton<ServiceBusPublisher>();
builder.Services.AddOpenApi();

var app = builder.Build();

app.MapControllers();

app.Run();
