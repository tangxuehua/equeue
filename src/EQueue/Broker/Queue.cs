using System;
using System.Collections.Concurrent;
using System.Threading;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class Queue
    {
        private ConcurrentDictionary<long, QueueItem> _queueMessageDict = new ConcurrentDictionary<long, QueueItem>();
        private long _currentOffset = -1;
        private long _maxRemovedOffset = -1;

        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long CurrentOffset { get { return _currentOffset; } }
        public long MaxRemovedOffset { get { return _maxRemovedOffset; } }
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
        public void SetQueueMessage(long queueOffset, QueueMessage queueMessage)
        {
            _queueMessageDict[queueOffset] = new QueueItem(queueMessage.MessageOffset, queueMessage.StoredTime);
        }
        public QueueItem GetQueueItem(long queueOffset)
        {
            QueueItem queueItem;
            if (_queueMessageDict.TryGetValue(queueOffset, out queueItem))
            {
                return queueItem;
            }
            return null;
        }
        public QueueItem RemoveQueueItem(long queueOffset)
        {
            QueueItem queueItem;
            if (_queueMessageDict.TryRemove(queueOffset, out queueItem))
            {
                if (queueOffset > _maxRemovedOffset)
                {
                    _maxRemovedOffset = queueOffset;
                }
                return queueItem;
            }
            return null;
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
