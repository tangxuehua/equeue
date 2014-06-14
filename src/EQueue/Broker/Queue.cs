using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using ECommon.Extensions;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class Queue
    {
        private ConcurrentDictionary<long, QueueItem> _queueItemDict = new ConcurrentDictionary<long, QueueItem>();
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
        public void RecoverQueueItem(QueueMessage queueMessage)
        {
            _queueItemDict[queueMessage.QueueOffset] = new QueueItem(queueMessage.MessageOffset, queueMessage.StoredTime);
            if (queueMessage.QueueOffset > _currentOffset)
            {
                _currentOffset = queueMessage.QueueOffset;
            }
        }
        public void AddQueueItem(QueueMessage queueMessage)
        {
            _queueItemDict[queueMessage.QueueOffset] = new QueueItem(queueMessage.MessageOffset, queueMessage.StoredTime);
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
        public QueueItem GetQueueItem(long queueOffset)
        {
            QueueItem queueItem;
            if (_queueItemDict.TryGetValue(queueOffset, out queueItem))
            {
                return queueItem;
            }
            return null;
        }
        public void RemoveQueueItems(long maxQueueOffset)
        {
            var toRemoveQueueOffsets = _queueItemDict.Keys.Where(key => key <= maxQueueOffset).ToList();
            toRemoveQueueOffsets.ForEach(queueOffset => _queueItemDict.Remove(queueOffset));
        }
    }

    public class QueueItem
    {
        public long MessageOffset { get; private set; }
        public DateTime StoredTime { get; private set; }

        public QueueItem(long messageOffset, DateTime storedTime)
        {
            MessageOffset = messageOffset;
            StoredTime = storedTime;
        }
    }
}
