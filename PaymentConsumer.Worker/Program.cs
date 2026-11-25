using Azure.Messaging.ServiceBus;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.EntityFrameworkCore;
using PaymentConsumer.Worker.Data;
using PaymentConsumer.Worker.Services;
using PaymentConsumer.Worker.Workers;
using Serilog;

var builder = Host.CreateApplicationBuilder(args);

// Serilog is the single logging provider.
// Existing ILogger<T> dependencies continue to work.

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
        .Enrich.WithProperty("ServiceName", "PaymentService")
        .WriteTo.ApplicationInsights(
            services.GetRequiredService<TelemetryConfiguration>(),
            TelemetryConverter.Traces);
});

var serviceBusConnectionString =
    builder.Configuration["ServiceBus:ConnectionString"]
    ?? throw new InvalidOperationException(
        "Service Bus connection string is missing.");

var paymentConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Payment database connection string is missing.");

builder.Services.AddSingleton(_ =>
    new ServiceBusClient(serviceBusConnectionString));

builder.Services.AddDbContext<PaymentDbContext>(options =>
    options.UseSqlServer(paymentConnectionString));

builder.Services.AddScoped<PaymentGatewayService>();
builder.Services.AddHostedService<PaymentProcessor>();

using var host = builder.Build();

try
{
    // Validate the service-owned Payment database at startup.
    await using (var scope = host.Services.CreateAsyncScope())
    {
        var dbContext = scope.ServiceProvider
            .GetRequiredService<PaymentDbContext>();

        var canConnect = await dbContext.Database.CanConnectAsync();

        if (!canConnect)
        {
            throw new InvalidOperationException(
                "Cannot connect to the Payment database.");
        }

        Log.Information(
            "Payment database connection verified. Database: {DatabaseName}",
            dbContext.Database.GetDbConnection().Database);
    }

    Log.Information("Starting Payment Consumer Worker");
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Payment Consumer Worker terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
