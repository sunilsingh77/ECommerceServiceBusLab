
using Microsoft.EntityFrameworkCore;
using PaymentConsumer.Worker.Models;
using System.Reflection.Emit;

namespace PaymentConsumer.Worker.Data;

public class PaymentDbContext : DbContext
{
    public PaymentDbContext(DbContextOptions<PaymentDbContext> options) : base(options)
    {
    }

    public DbSet<PaymentTransaction> PaymentTransactions => Set<PaymentTransaction>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<PaymentTransaction>(entity =>
        {
            entity.ToTable("PaymentTransactions");

            entity.HasKey(x => x.PaymentId);

            entity.HasIndex(x => x.OrderId)
                .IsUnique();

            entity.Property(x => x.CustomerName)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.Amount)
                .HasColumnType("decimal(18,2)");

            entity.Property(x => x.PaymentStatus)
                .HasMaxLength(30)
                .IsRequired();

            entity.Property(x => x.PaymentReference)
                .HasMaxLength(100);
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
