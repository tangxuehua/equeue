using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using ECommon.Extensions;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class Queue
    {
        private ConcurrentDictionary<long, long> _queueItemDict = new ConcurrentDictionary<long, long>();
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
        public void RecoverQueueItem(long queueOffset, long messageOffset)
        {
            _queueItemDict[queueOffset] = messageOffset;
            if (queueOffset > _currentOffset)
            {
                _currentOffset = queueOffset;
            }
        }
        public void AddQueueItem(long queueOffset, long messageOffset)
        {
            _queueItemDict[queueOffset] = messageOffset;
        }
        public long? GetMinQueueOffset()
        {
            long? minOffset = null;
            foreach (var key in _queueItemDict.Keys)
            {
                if (minOffset == null)
                {
                    minOffset = key;
                }
                else if (key < minOffset.Value)
                {
                    minOffset = key;
                }
            }
            return minOffset;
        }
        public long GetMessageOffset(long queueOffset)
        {
            long messageOffset;
            if (_queueItemDict.TryGetValue(queueOffset, out messageOffset))
            {
                return messageOffset;
            }
            return -1;
        }
        public void RemoveQueueItems(long maxQueueOffset)
        {
            var toRemoveQueueOffsets = _queueItemDict.Keys.Where(key => key <= maxQueueOffset).ToList();
            toRemoveQueueOffsets.ForEach(queueOffset => _queueItemDict.Remove(queueOffset));
        }
    }
}
