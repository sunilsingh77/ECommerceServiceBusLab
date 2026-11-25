using Azure.Messaging.ServiceBus;
using InventoryConsumer.Worker.Data;
using InventoryConsumer.Worker.Workers;
using Microsoft.EntityFrameworkCore;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);


// Use Serilog as the single logging provider.
// Existing ILogger<T> injections continue to work.
builder.Logging.ClearProviders();

// Read Application Insights connection string from configuration/User Secrets.
var applicationInsightsConnectionString =
    builder.Configuration["ApplicationInsights:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Application Insights connection string is missing. Configure it using .NET User Secrets.");

// Register Application Insights Worker Service SDK.
builder.Services.AddApplicationInsightsTelemetryWorkerService(options =>
{
    options.ConnectionString = applicationInsightsConnectionString;
});
// Existing ILogger<T> injections continue to work.
builder.Services.AddSerilog((services, loggerConfiguration) =>
{
    loggerConfiguration
        .ReadFrom.Configuration(builder.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("ServiceName", "InventoryService");
});

var inventoryConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Inventory database connection string is missing.");

var serviceBusConnectionString =
    builder.Configuration["ServiceBus:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Service Bus connection string is missing.");

builder.Services.AddDbContext<InventoryDbContext>(options =>
    options.UseSqlServer(inventoryConnectionString));

builder.Services.AddSingleton(_ =>
    new ServiceBusClient(serviceBusConnectionString));

builder.Services.AddHostedService<InventoryProcessor>();

var host = builder.Build();

await host.RunAsync();
