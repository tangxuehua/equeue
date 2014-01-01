using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        MessageStoreResult StoreMessage(Message message, int queueId, long queueOffset);
        QueueMessage GetMessage(long offset);
        void Recover();
    }
}
