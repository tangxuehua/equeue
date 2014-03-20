using System.Collections.Concurrent;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers.OffsetStores
{
    public class InMemoryOffsetStore : IOffsetStore
    {
        private ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _dict = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();

        public void UpdateQueueOffset(string groupName, MessageQueue messageQueue, long queueOffset)
        {
            var queueOffsetDict = _dict.GetOrAdd(groupName, new ConcurrentDictionary<string, long>());
            var key = string.Format("{0}-{1}", messageQueue.Topic, messageQueue.QueueId);
            queueOffsetDict.AddOrUpdate(key, queueOffset, (currentKey, oldOffset) =>
            {
                return queueOffset > oldOffset ? queueOffset : oldOffset;
            });
        }
        public long GetQueueOffset(string groupName, MessageQueue messageQueue)
        {
            ConcurrentDictionary<string, long> queueOffsetDict;
            if (_dict.TryGetValue(groupName, out queueOffsetDict))
            {
                long queueOffset;
                if (queueOffsetDict.TryGetValue(string.Format("{0}-{1}", messageQueue.Topic, messageQueue.QueueId), out queueOffset))
                {
                    return queueOffset;
                }
            }
            return -1;
        }
        public virtual void PersistQueueOffset(string groupName, MessageQueue messageQueue) { }
    }
}
