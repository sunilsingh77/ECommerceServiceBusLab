
namespace PaymentConsumer.Worker.Exceptions;

public class PermanentPaymentException : Exception
{
    public PermanentPaymentException(string message)
        : base(message)
    {
    }
}
