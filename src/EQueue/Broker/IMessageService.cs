using EQueue.Common;

namespace EQueue.Broker
{
    public interface IMessageService
    {
        MessageStoreResult StoreMessage(Message message, string arg);
        QueueMessage GetMessage(string topic, int queueId, long queueOffset);
        long GetQueueCurrentOffset(string topic, int queueId);
    }
}
