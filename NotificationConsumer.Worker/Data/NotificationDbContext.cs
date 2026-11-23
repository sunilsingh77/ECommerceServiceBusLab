using Microsoft.EntityFrameworkCore;
using NotificationConsumer.Worker.Models;

namespace NotificationConsumer.Worker.Data;

public class NotificationDbContext : DbContext
{
    public NotificationDbContext(DbContextOptions<NotificationDbContext> options) : base(options)
    {
    }

    public DbSet<Notification> Notifications =>
        Set<Notification>();

    public DbSet<InboxMessage> InboxMessages =>
        Set<InboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Notification>(entity =>
        {
            entity.ToTable("Notifications");

            entity.HasKey(x => x.NotificationId);

            entity.HasIndex(x => new
            {
                x.OrderId,
                x.NotificationType
            }).IsUnique();

            entity.Property(x => x.CustomerName)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.NotificationType)
                .HasMaxLength(50)
                .IsRequired();

            entity.Property(x => x.NotificationStatus)
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