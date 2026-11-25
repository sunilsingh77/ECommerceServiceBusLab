namespace InventoryConsumer.Worker.Models;

public class InventoryReservation
{
    public Guid ReservationId { get; set; }
    public Guid OrderId { get; set; }
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public string ReservationStatus { get; set; } = "Reserved";
    public DateTime CreatedAt { get; set; }
    public string? CorrelationId { get; set; }
}