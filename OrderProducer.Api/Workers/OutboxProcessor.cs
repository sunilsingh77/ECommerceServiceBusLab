using System.Text.Json;
using ECommerce.Contracts;
using Microsoft.EntityFrameworkCore;
using OrderProducer.Api.Data;
using OrderProducer.Api.Services;

namespace OrderProducer.Api.Workers;

public class OutboxProcessor : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(IServiceScopeFactory scopeFactory, ILogger<OutboxProcessor> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Outbox Processor started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessPendingMessagesAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Unexpected error in Outbox Processor.");
            }
            await Task.Delay(
                TimeSpan.FromSeconds(5),
                stoppingToken);
        }
        _logger.LogInformation("Outbox Processor stopped.");
    }

    private async Task ProcessPendingMessagesAsync(CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();

        var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        var publisher = scope.ServiceProvider.GetRequiredService<ServiceBusPublisher>();

        var pendingMessages = await dbContext.OutboxMessages
                                            .Where(x => x.ProcessedOnUtc == null)
                                            .OrderBy(x => x.OccurredOnUtc)
                                            .Take(10)
                                            .ToListAsync(cancellationToken);

        foreach (var outboxMessage in pendingMessages)
        {
            try
            {
                if (outboxMessage.Type != nameof(OrderCreatedEvent))
                {
                    throw new InvalidOperationException($"Unsupported event type: {outboxMessage.Type}");
                }

                var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(outboxMessage.Content) 
                    ?? throw new InvalidOperationException("Could not deserialize OrderCreatedEvent.");

                // Publish to the existing orders-topic.
                await publisher.PublishOrderToTopicAsync(orderEvent, outboxMessage.CorrelationId, cancellationToken);

                // Mark as processed only after successful publishing.
                outboxMessage.ProcessedOnUtc = DateTime.UtcNow;
                outboxMessage.Error = null;

                await dbContext.SaveChangesAsync(cancellationToken);

                _logger.LogInformation("Outbox event {EventId} published successfully. OrderId: {OrderId}, CorrelationId: {CorrelationId}", outboxMessage.Id, orderEvent.OrderId, outboxMessage.CorrelationId);
            }
            catch (OperationCanceledException)  when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                outboxMessage.Error = ex.Message;
                await dbContext.SaveChangesAsync(cancellationToken);

                _logger.LogError(
                    ex,
                    "Failed to publish Outbox event {EventId}. It will be retried.",
                    outboxMessage.Id);
            }
        }
    }
}