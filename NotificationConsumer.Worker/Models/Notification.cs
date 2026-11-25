namespace NotificationConsumer.Worker.Models;

public class Notification
{
    public Guid NotificationId { get; set; }
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public string NotificationType { get; set; } = string.Empty;
    public string NotificationStatus { get; set; } = "Pending";
    public DateTime CreatedAt { get; set; }
    public DateTime? SentAt { get; set; }
    public string? CorrelationId { get; set; }
}