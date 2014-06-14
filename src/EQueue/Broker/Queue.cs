using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        public void RecoverQueueItem(QueueMessage queueMessage)
        {
            _queueMessageDict[queueMessage.QueueOffset] = new QueueItem(queueMessage.MessageOffset, queueMessage.StoredTime);
            if (queueMessage.QueueOffset > _currentOffset)
            {
                _currentOffset = queueMessage.QueueOffset;
            }
            if (_maxRemovedOffset == -1)
            {
                _maxRemovedOffset = queueMessage.QueueOffset - 1;
            }
        }
        public void AddQueueItem(QueueMessage queueMessage)
        {
            _queueMessageDict[queueMessage.QueueOffset] = new QueueItem(queueMessage.MessageOffset, queueMessage.StoredTime);
        }
        public long? GetMinQueueOffset()
        {
            long? minOffset = null;
            foreach (var key in _queueMessageDict.Keys)
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
            if (_queueMessageDict.TryGetValue(queueOffset, out queueItem))
            {
                return queueItem;
            }
            return null;
        }
        public IEnumerable<QueueItem> RemoveQueueItems(long maxQueueOffset)
        {
            var toRemoveEntries = new List<KeyValuePair<long, QueueItem>>();
            foreach (var entry in _queueMessageDict)
            {
                if (entry.Key <= maxQueueOffset)
                {
                    toRemoveEntries.Add(entry);
                }
            }

            var removedQueueItems = new List<QueueItem>();
            foreach (var entry in toRemoveEntries)
            {
                QueueItem queueItem;
                if (_queueMessageDict.TryRemove(entry.Key, out queueItem))
                {
                    removedQueueItems.Add(queueItem);
                }
            }

            return removedQueueItems;
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
