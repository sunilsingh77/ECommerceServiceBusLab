
using Azure.Messaging.ServiceBus;
using Microsoft.EntityFrameworkCore;
using OrderProducer.Api.Data;
using OrderProducer.Api.Services;
using OrderProducer.Api.Workers;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddControllers();

var orderDbConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "OrderDb connection string missing.");
builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseSqlServer(orderDbConnectionString));

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

builder.Services.AddSingleton<ServiceBusPublisher>();
builder.Services.AddHostedService<OutboxProcessor>();

builder.Services.AddOpenApi();

var app = builder.Build();

app.MapControllers();

app.Run();
