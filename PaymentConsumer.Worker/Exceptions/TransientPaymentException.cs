
namespace PaymentConsumer.Worker.Exceptions;

public class TransientPaymentException : Exception
{
    public TransientPaymentException(string message)
        : base(message)
    {
    }
}
