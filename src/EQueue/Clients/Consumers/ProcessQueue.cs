using System.Collections.Generic;
using System.Linq;
using ECommon.Extensions;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ProcessQueue
    {
        private SortedDictionary<long, QueueMessage> _messageDict = new SortedDictionary<long, QueueMessage>();
        private long _consumedQueueOffset = -1L;
        private long _previousConsumedQueueOffset = -1L;

        public bool IsDropped { get; set; }

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
            lock (this)
            {
                foreach (var message in messages)
                {
                    _messageDict[message.QueueOffset] = message;
                }
            }
        }
        public void RemoveMessage(QueueMessage message)
        {
            lock (this)
            {
                if (_messageDict.Remove(message.QueueOffset))
                {
                    if (_messageDict.Keys.IsNotEmpty())
                    {
                        _consumedQueueOffset = _messageDict.Keys.First() - 1;
                    }
                    else
                    {
                        _consumedQueueOffset = message.QueueOffset;
                    }
                }
            }
        }
        public int GetMessageCount()
        {
            lock (this)
            {
                return _messageDict.Count;
            }
        }
        public long GetConsumedQueueOffset()
        {
            return _consumedQueueOffset;
        }
    }
}
