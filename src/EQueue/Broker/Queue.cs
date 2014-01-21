using System;
using System.Collections.Concurrent;
using System.Threading;

namespace EQueue.Broker
{
    [Serializable]
    public class Queue
    {
        private ConcurrentDictionary<long, long> _queueOffsetMappingDict = new ConcurrentDictionary<long, long>();
        private long _currentOffset = -1;

        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long CurrentOffset { get { return _currentOffset; } }
        public QueueStatus Status { get; private set; }

        public Queue(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
        }

        public void AllowEnqueue()
        {
            Status = QueueStatus.Normal;
        }
        public void DisableEnqueue()
        {
            Status = QueueStatus.EnqueueDisabled;
        }
        public long IncrementCurrentOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
        public void SetMessageOffset(long queueOffset, long messageOffset)
        {
            _queueOffsetMappingDict[queueOffset] = messageOffset;
        }
        public long GetMessageOffset(long queueOffset)
        {
            long offset;
            if (_queueOffsetMappingDict.TryGetValue(queueOffset, out offset))
            {
                return offset;
            }
            return -1;
        }
    }
}
