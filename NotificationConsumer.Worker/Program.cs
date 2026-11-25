using Azure.Messaging.ServiceBus;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.EntityFrameworkCore;
using NotificationConsumer.Worker.Data;
using NotificationConsumer.Worker.Workers;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

var applicationInsightsConnectionString =
    builder.Configuration["ApplicationInsights:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Application Insights connection string is missing.");

builder.Services.AddApplicationInsightsTelemetryWorkerService(options =>
{
    options.ConnectionString = applicationInsightsConnectionString;
});

builder.Logging.ClearProviders();

builder.Services.AddSerilog((services, loggerConfiguration) =>
{
    loggerConfiguration
        .ReadFrom.Configuration(builder.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("ServiceName", "NotificationService")
        .WriteTo.ApplicationInsights(
            services.GetRequiredService<TelemetryConfiguration>(),
            TelemetryConverter.Traces);
});

var serviceBusConnectionString =
    builder.Configuration["ServiceBus:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Service Bus connection string missing. Configure it using user-secrets.");

var notificationConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Notification database connection string missing.");

builder.Services.AddSingleton(_ => new ServiceBusClient(serviceBusConnectionString));

builder.Services.AddDbContext<NotificationDbContext>(options =>
    options.UseSqlServer(notificationConnectionString));

builder.Services.AddHostedService<NotificationProcessor>();

var host = builder.Build();

try
{
    Log.Information("Starting Notification Consumer Worker");
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Notification Consumer Worker terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
