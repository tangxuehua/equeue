using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public interface IMessageContext
    {
        void OnMessageHandled(QueueMessage queueMessage);
    }
}
