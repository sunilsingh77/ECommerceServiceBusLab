# PaymentConsumer.Worker — Serilog clean replacement files

Replace these files in the existing PaymentConsumer.Worker project:

- `Program.cs`
- `appsettings.json`
- `Workers/PaymentProcessor.cs`
- `Services/PaymentGatewayService.cs`

The uploaded `.csproj` already contains the required Serilog packages:
- Serilog.Extensions.Hosting
- Serilog.Settings.Configuration
- Serilog.Sinks.Console
- Serilog.Sinks.File
- Serilog.Formatting.Compact
- Serilog.Enrichers.Environment
- Serilog.Enrichers.Process
- Serilog.Enrichers.Thread

No `.csproj` package changes are required based on the supplied project file.

## Configuration

The supplied appsettings retains the local SQL Server database:
`ECommerce_PaymentDb`

It configures:
- Topic: `orders-topic`
- Subscription: `payment-sub`
- Retry queue: `payment-retry-queue`

Set the Service Bus connection string using User Secrets:

```powershell
dotnet user-secrets set "ServiceBus:ConnectionString" "<your-service-bus-connection-string>" --project PaymentConsumer.Worker
```

Do not put the Service Bus connection string in appsettings.json.

## Build and run

From the solution directory:

```powershell
dotnet clean PaymentConsumer.Worker
dotnet build PaymentConsumer.Worker
dotnet run --project PaymentConsumer.Worker
```

Daily compact JSON log files are written to:
`PaymentConsumer.Worker/logs/`

## Behavior preserved

- Payment service owns and uses `ECommerce_PaymentDb`.
- PaymentTransactions and InboxMessages remain in the same local SQL transaction.
- Inbox idempotency by original event identity is preserved.
- Retry count and 5/10/20-second retry schedule are preserved.
- Retry message keeps the original CorrelationId, including fallback from the CorrelationId application property.
- OriginalMessageId is retained across retry/replay.
- Permanent `INVALID` payment test remains.
- `TEMPFAIL` transient failure simulation remains commented out, as in the supplied source.
- `CRASHONCE` post-commit simulation remains.
- Message is completed only after successful database commit or successful retry scheduling.

## Important verification

The clean files are based on the uploaded source files, but have not been compiled against the user's local solution or run against Azure Service Bus/SQL Server here. Build and test before replacing the Notification worker.

The payment gateway in this lab is simulated. A production payment provider needs its own idempotency key and reconciliation strategy; a local SQL unique constraint alone cannot guarantee exactly-once external charging.
