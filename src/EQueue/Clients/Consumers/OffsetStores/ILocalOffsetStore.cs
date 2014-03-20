using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public interface ILocalOffsetStore : IOffsetStore
    {
        void PersistQueueOffset(string groupName, MessageQueue messageQueue);
    }
}
