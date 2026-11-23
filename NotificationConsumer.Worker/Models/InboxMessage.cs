namespace NotificationConsumer.Worker.Models;

public class InboxMessage
{
    public string MessageId { get; set; } = string.Empty;
    public DateTime ProcessedOnUtc { get; set; }
    public string ConsumerName { get; set; } = string.Empty;
}