using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public interface IOffsetStore
    {
        void Load();
        void UpdateOffset(MessageQueue messageQueue, long offset);
        long ReadOffset(MessageQueue messageQueue, OffsetReadType readType);
        void Persist(MessageQueue messageQueue);
        void RemoveOffset(MessageQueue messageQueue);
    }
}
