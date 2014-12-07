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
        public long GetMessageCount()
        {
            return _queueItemDict.Count;
        }
        public long GetMessageRealCount()
        {
            var minOffset = GetMinQueueOffset();
            if (minOffset == null)
            {
                return 0L;
            }
            return _currentOffset - minOffset.Value + 1;
        }
        public QueueStatus Status { get; private set; }

        public Queue(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
            Status = QueueStatus.Normal;
        }

        public void Enable()
        {
            Status = QueueStatus.Normal;
        }
        public void Disable()
        {
            Status = QueueStatus.Disabled;
        }
        public long IncrementCurrentOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
        public void RecoverQueueIndex(long queueOffset, long messageOffset, bool allowSetQueueIndex)
        {
            if (allowSetQueueIndex)
            {
                SetQueueIndex(queueOffset, messageOffset);
            }
            if (queueOffset > _currentOffset)
            {
                _currentOffset = queueOffset;
            }
        }
        public void SetQueueIndex(long queueOffset, long messageOffset)
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
        public long RemoveConsumedQueueIndex(long maxConsumedQueueOffset)
        {
            var totalRemovedCount = 0L;
            var allRemoveQueueOffsets = _queueItemDict.Keys.Where(key => key <= maxConsumedQueueOffset);
            foreach (var consumedQueueOffset in allRemoveQueueOffsets)
            {
                long messageOffset;
                if (_queueItemDict.TryRemove(consumedQueueOffset, out messageOffset))
                {
                    totalRemovedCount++;
                }
            }
            return totalRemovedCount;
        }
        public long RemoveLastQueueIndex(long requireRemoveCount)
        {
            var queueOffset = _queueItemDict.Keys.LastOrDefault();
            var totalRemovedCount = 0L;
            while (queueOffset >= 0L && totalRemovedCount < requireRemoveCount)
            {
                long messageOffset;
                if (_queueItemDict.TryRemove(queueOffset, out messageOffset))
                {
                    totalRemovedCount++;
                }
                queueOffset--;
            }
            return totalRemovedCount;
        }
    }
}
