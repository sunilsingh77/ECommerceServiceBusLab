
using Azure.Messaging.ServiceBus;
using InventoryConsumer.Worker.Workers;
using InventoryConsumer.Worker.Data;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

var inventoryConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Inventory database connection string missing.");

builder.Services.AddDbContext<InventoryDbContext>(options =>
    options.UseSqlServer(inventoryConnectionString));

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString = builder.Configuration["ServiceBus:ConnectionString"] ?? throw new InvalidOperationException("Service Bus connection string missing.");
    return new ServiceBusClient(connectionString);
});

builder.Services.AddHostedService<InventoryProcessor>();
var host = builder.Build();

host.Run();
