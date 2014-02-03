using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public class DefaultLocalOffsetStore : ILocalOffsetStore
    {
        public void Load()
        {

        }

        public void UpdateOffset(MessageQueue messageQueue, long offset)
        {

        }

        public long ReadOffset(MessageQueue messageQueue, OffsetReadType readType)
        {
            return 0L;
        }

        public void Persist(MessageQueue messageQueue)
        {

        }

        public void RemoveOffset(MessageQueue messageQueue)
        {

        }
    }
}
