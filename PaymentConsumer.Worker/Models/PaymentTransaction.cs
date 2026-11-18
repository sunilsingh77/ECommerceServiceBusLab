
namespace PaymentConsumer.Worker.Models;

public class PaymentTransaction
{
    public Guid PaymentId { get; set; }
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string PaymentStatus { get; set; } = "Pending";
    public string? PaymentReference { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
}
