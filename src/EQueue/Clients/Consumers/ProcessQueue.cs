using System.Collections.Generic;
using System.Linq;
using ECommon.Extensions;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ProcessQueue
    {
        private readonly object _lockObj = new object();
        private readonly SortedDictionary<long, QueueMessage> _messageDict = new SortedDictionary<long, QueueMessage>();
        private long _consumedQueueOffset = -1L;
        private long _previousConsumedQueueOffset = -1L;
        private long _maxQueueOffset = -1;
        private int _messageCount = 0;

        public bool IsDropped { get; set; }

        public void Reset()
        {
            lock (_lockObj)
            {
                _messageDict.Clear();
                _consumedQueueOffset = -1L;
                _previousConsumedQueueOffset = -1L;
                _maxQueueOffset = -1;
                _messageCount = 0;
            }
        }
        public bool TryUpdatePreviousConsumedQueueOffset(long current)
        {
            if (current != _previousConsumedQueueOffset)
            {
                _previousConsumedQueueOffset = current;
                return true;
            }
            return false;
        }
        public void AddMessages(IEnumerable<QueueMessage> messages)
        {
            lock (_lockObj)
            {
                foreach (var message in messages)
                {
                    if (_messageDict.ContainsKey(message.QueueOffset))
                    {
                        continue;
                    }
                    _messageDict[message.QueueOffset] = message;
                    if (_maxQueueOffset == -1 && message.QueueOffset >= 0)
                    {
                        _maxQueueOffset = message.QueueOffset;
                    }
                    else if (message.QueueOffset > _maxQueueOffset)
                    {
                        _maxQueueOffset = message.QueueOffset;
                    }
                    _messageCount++;
                }
            }
        }
        public void RemoveMessage(QueueMessage message)
        {
            lock (_lockObj)
            {
                if (_messageDict.Remove(message.QueueOffset))
                {
                    if (_messageDict.Keys.IsNotEmpty())
                    {
                        _consumedQueueOffset = _messageDict.Keys.First() - 1;
                    }
                    else
                    {
                        _consumedQueueOffset = _maxQueueOffset;
                    }
                    _messageCount--;
                }
            }
        }
        public int GetMessageCount()
        {
            return _messageCount;
        }
        public long GetConsumedQueueOffset()
        {
            return _consumedQueueOffset;
        }
    }
}
