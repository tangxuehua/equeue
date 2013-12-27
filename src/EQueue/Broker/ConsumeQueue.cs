using System;
using System.Collections.Concurrent;
using System.Threading;

namespace EQueue.Common
{
    [Serializable]
    public class ConsumeQueue
    {
        private ConcurrentDictionary<long, long> _queueOffsetMappingDict = new ConcurrentDictionary<long, long>();
        private long _currentOffset = -1;

        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public ConsumeQueue(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
        }

        public long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
    }
}
