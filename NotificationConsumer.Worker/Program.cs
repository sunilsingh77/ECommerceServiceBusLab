
using Azure.Messaging.ServiceBus;
using NotificationConsumer.Worker.Workers;
using Microsoft.EntityFrameworkCore;
using NotificationConsumer.Worker.Data;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString =
        builder.Configuration["ServiceBus:ConnectionString"]
        ?? throw new InvalidOperationException(
            "Service Bus connection string missing.");

    return new ServiceBusClient(connectionString);
});

var notificationConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Notification database connection string missing.");

builder.Services.AddDbContext<NotificationDbContext>(options =>
    options.UseSqlServer(notificationConnectionString));

builder.Services.AddHostedService<NotificationProcessor>();

var host = builder.Build();

host.Run();
