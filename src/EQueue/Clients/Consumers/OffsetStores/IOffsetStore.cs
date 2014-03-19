using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public interface IOffsetStore
    {
        void Start();
        void UpdateOffset(string groupName, MessageQueue messageQueue, long queueOffset);
        void PersistOffset(string groupName, MessageQueue messageQueue);
    }
}
