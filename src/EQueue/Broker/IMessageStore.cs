using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        QueueMessage StoreMessage(Message message, int queueId, long queueOffset);
        QueueMessage GetMessage(long offset);
        bool RemoveMessage(long offset);
        void Recover();
    }
}
