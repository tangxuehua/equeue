using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public interface ILocalOffsetStore
    {
        void PersistQueueOffset(string groupName, MessageQueue messageQueue, long queueOffset);
        long GetQueueOffset(string groupName, MessageQueue messageQueue);
    }
}
