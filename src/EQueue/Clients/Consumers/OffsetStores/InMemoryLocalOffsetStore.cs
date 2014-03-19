using System.Collections.Concurrent;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public class InMemoryLocalOffsetStore : ILocalOffsetStore
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _dict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();

        public void Start() { }
        public void UpdateOffset(string groupName, MessageQueue messageQueue, long nextOffset)
        {
            var queueOffsetDict = _dict.GetOrAdd(groupName, new ConcurrentDictionary<string, long>());
            var key = string.Format("{0}-{1}", messageQueue.Topic, messageQueue.QueueId);
            queueOffsetDict.AddOrUpdate(key, nextOffset, (currentKey, oldNextOffset) =>
            {
                return nextOffset > oldNextOffset ? nextOffset : oldNextOffset;
            });
        }
        public void PersistOffset(string groupName, MessageQueue messageQueue) { }
    }
}
