using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public interface IOffsetStore
    {
        void UpdateQueueOffset(string groupName, MessageQueue messageQueue, long queueOffset);
        long GetQueueOffset(string groupName, MessageQueue messageQueue);
    }
}
