using Microsoft.EntityFrameworkCore;
using OrderProducer.Api.Models;
using System.Reflection.Emit;

namespace OrderProducer.Api.Data;

public class OrderDbContext : DbContext
{
    public OrderDbContext(
        DbContextOptions<OrderDbContext> options)
        : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();

    public DbSet<OutboxMessage> OutboxMessages =>
        Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.ToTable("Orders");

            entity.HasKey(x => x.OrderId);

            entity.Property(x => x.CustomerName)
                .HasMaxLength(100)
                .IsRequired();

            entity.Property(x => x.TotalAmount)
                .HasColumnType("decimal(18,2)");
        });

        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("OutboxMessages");

            entity.HasKey(x => x.Id);

            entity.Property(x => x.Type)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.Content)
                .HasColumnType("nvarchar(max)")
                .IsRequired();
        });
    }
}