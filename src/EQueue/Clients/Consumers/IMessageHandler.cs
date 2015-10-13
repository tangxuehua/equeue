using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public interface IMessageHandler
    {
        void Handle(QueueMessage message, IMessageContext context);
    }
}
