using System.Collections.Generic;
using System.Linq;
using ECommon.Extensions;

namespace EQueue.Clients.Consumers
{
    public class ProcessQueue
    {
        private readonly object _lockObj = new object();
        private readonly SortedDictionary<long, ConsumingMessage> _messageDict = new SortedDictionary<long, ConsumingMessage>();
        private long _consumedQueueOffset = -1L;
        private long _previousConsumedQueueOffset = -1L;
        private long _maxQueueOffset = -1;
        private int _messageCount = 0;

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
        public void MarkAllConsumingMessageIgnored()
        {
            lock (_lockObj)
            {
                var existingConsumingMessages = _messageDict.Values.ToList();
                foreach (var consumingMessage in existingConsumingMessages)
                {
                    consumingMessage.IsIgnored = true;
                }
            }
        }
        public void AddMessages(IEnumerable<ConsumingMessage> consumingMessages)
        {
            lock (_lockObj)
            {
                foreach (var consumingMessage in consumingMessages)
                {
                    if (_messageDict.ContainsKey(consumingMessage.Message.QueueOffset))
                    {
                        continue;
                    }
                    _messageDict[consumingMessage.Message.QueueOffset] = consumingMessage;
                    if (_maxQueueOffset == -1 && consumingMessage.Message.QueueOffset >= 0)
                    {
                        _maxQueueOffset = consumingMessage.Message.QueueOffset;
                    }
                    else if (consumingMessage.Message.QueueOffset > _maxQueueOffset)
                    {
                        _maxQueueOffset = consumingMessage.Message.QueueOffset;
                    }
                    _messageCount++;
                }
            }
        }
        public void RemoveMessage(ConsumingMessage consumingMessage)
        {
            lock (_lockObj)
            {
                if (_messageDict.Remove(consumingMessage.Message.QueueOffset))
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
