
using Azure.Messaging.ServiceBus;
using PaymentConsumer.Worker.Data;
using PaymentConsumer.Worker.Services;
using PaymentConsumer.Worker.Workers;
using Microsoft.EntityFrameworkCore;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ServiceBusClient>(_ =>
{
    var connectionString =
        builder.Configuration["ServiceBus:ConnectionString"]
        ?? throw new InvalidOperationException(
            "Service Bus connection string missing.");

    return new ServiceBusClient(connectionString);
});
var paymentConnectionString =
    builder.Configuration.GetConnectionString("DefaultConnection")
    ?? throw new InvalidOperationException(
        "Payment database connection string missing.");

builder.Services.AddDbContext<PaymentDbContext>(options => options.UseSqlServer(paymentConnectionString));

builder.Services.AddScoped<PaymentGatewayService>();
builder.Services.AddHostedService<PaymentProcessor>();

var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider
        .GetRequiredService<PaymentDbContext>();

    var canConnect = await dbContext.Database.CanConnectAsync();

    Console.WriteLine(
        $"Payment Database Connection: {canConnect}");
}

host.Run();