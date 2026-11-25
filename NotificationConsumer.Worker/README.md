# NotificationConsumer.Worker — Serilog integration

## Files
Replace the matching files in `NotificationConsumer.Worker`:
- `NotificationConsumer.Worker.csproj`
- `Program.cs`
- `NotificationProcessor.cs`
- `appsettings.json`

## Service configuration
The worker reads the Service Bus connection string from configuration key:
`ServiceBus:ConnectionString`

Recommended local setup using User Secrets (run from the solution folder):
```powershell
dotnet user-secrets set "ServiceBus:ConnectionString" "YOUR_CONNECTION_STRING" --project NotificationConsumer.Worker
```

The local SQL Server connection remains pointed at `ECommerce_NotificationDb`.
Do not move Notification tables into another service database.

## Build and run
```powershell
dotnet clean NotificationConsumer.Worker
dotnet restore NotificationConsumer.Worker
dotnet build NotificationConsumer.Worker
dotnet run --project NotificationConsumer.Worker
```

## Expected log properties
JSON console output and daily rolling files under:
`logs/NotificationService-.json`

Expected common properties include:
- `ServiceName`: `NotificationService`
- `EnvironmentName`, `MachineName`, `ProcessId`, `ThreadId`
- `CorrelationId`, `MessageId`, `DeliveryCount`
- `Consumer`, `EntityPath`
- `OrderId` on order-specific events

## Preserved processing behavior
- `notification-sub` subscription on `orders-topic`
- Inbox idempotency using `OriginalMessageId` when present
- Notification + Inbox persisted in one local SQL transaction
- Service Bus message completed after the SQL transaction commits
- Invalid order data is dead-lettered; processing failures are abandoned for retry
