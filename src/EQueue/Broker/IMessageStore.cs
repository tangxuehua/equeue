using EQueue.Common;

namespace EQueue.Broker
{
    public interface IMessageStore
    {
        MessageStoreResult StoreMessage(Message message, int queueId, long queueOffset);
    }
}
