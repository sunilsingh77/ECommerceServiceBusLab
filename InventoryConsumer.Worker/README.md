# InventoryConsumer.Worker — Serilog test files

Replace the matching files in your existing `InventoryConsumer.Worker` project:
- `Program.cs`
- `appsettings.json`
- `Workers/InventoryProcessor.cs`

The existing `.csproj` already contains the Serilog packages required by this configuration:
- Serilog.Extensions.Hosting
- Serilog.Settings.Configuration
- Serilog.Sinks.Console
- Serilog.Sinks.File
- Serilog.Formatting.Compact
- Serilog.Enrichers.Environment
- Serilog.Enrichers.Process
- Serilog.Enrichers.Thread

## Service Bus connection string

The supplied appsettings intentionally does not contain a Service Bus connection string.
Keep it in User Secrets:

```powershell
dotnet user-secrets set "ServiceBus:ConnectionString" "<your-service-bus-connection-string>" --project InventoryConsumer.Worker
```

The appsettings uses:
- Topic: `orders-topic`
- Subscription: `inventory-sub`

The SQL connection string is retained from your uploaded configuration and points to the local `ECommerce_InventoryDb`.

## Test

From the solution directory:

```powershell
dotnet build InventoryConsumer.Worker
dotnet run --project InventoryConsumer.Worker
```

Send an order through the existing OrderProducer API/Outbox path. The Inventory subscriber should receive it from `orders-topic`.

Expected logs:
- Compact JSON on the console
- Daily compact JSON files under `InventoryConsumer.Worker/logs/`
- Scope properties: `CorrelationId`, `MessageId`, `DeliveryCount`, `Consumer`, `EntityPath`
- `OrderId` in the nested order logging scope

## Important notes

- The existing `ILogger<InventoryProcessor>` API is retained; Serilog is the provider.
- `AddSimpleConsole` was removed so the worker does not write duplicate logs through two logging providers.
- Existing Inbox/idempotency and SQL transaction ordering are preserved.
- The `OriginalMessageId` replay identity logic is preserved.
- Verify that the database schema contains the `CorrelationId` columns used by your current entity models.
- This is a code review and file preparation; the files have not been compiled against your local solution or run against your Azure resources here.
