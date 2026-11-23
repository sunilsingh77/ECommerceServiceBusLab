using InventoryConsumer.Worker.Models;
using Microsoft.EntityFrameworkCore;

namespace InventoryConsumer.Worker.Data;

public class InventoryDbContext : DbContext
{
    public InventoryDbContext(
        DbContextOptions<InventoryDbContext> options)
        : base(options)
    {
    }

    public DbSet<InventoryReservation> InventoryReservations =>
        Set<InventoryReservation>();

    public DbSet<InboxMessage> InboxMessages =>
        Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InventoryReservation>(entity =>
        {
            entity.ToTable("InventoryReservations");

            entity.HasKey(x => x.ReservationId);

            entity.HasIndex(x => x.OrderId).IsUnique();

            entity.Property(x => x.CustomerName)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.TotalAmount)
                .HasColumnType("decimal(18,2)");

            entity.Property(x => x.ReservationStatus)
                .HasMaxLength(30)
                .IsRequired();
        });

        modelBuilder.Entity<InboxMessage>(entity =>
        {
            entity.ToTable("InboxMessages");

            entity.HasKey(x => x.MessageId);

            entity.Property(x => x.MessageId)
                .HasMaxLength(128)
                .IsRequired();

            entity.Property(x => x.ConsumerName)
                .HasMaxLength(100)
                .IsRequired();
        });
    }
}