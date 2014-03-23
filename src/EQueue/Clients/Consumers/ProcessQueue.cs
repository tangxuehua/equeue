using System.Collections.Concurrent;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ProcessQueue
    {
        private ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private long _queueMaxOffset = -1L;

        public void AddMessages(IEnumerable<QueueMessage> messages)
        {
            foreach (var message in messages)
            {
                if (_messageDict.TryAdd(message.QueueOffset, message))
                {
                    _queueMaxOffset = message.QueueOffset;
                }
            }
        }
        public void RemoveMessage(QueueMessage message)
        {
            QueueMessage removedMessage;
            _messageDict.TryRemove(message.QueueOffset, out removedMessage);
        }
        public int GetMessageCount()
        {
            return _messageDict.Count;
        }
        public long GetConsumedMinQueueOffset()
        {
            var currentMinQueueOffset = _queueMaxOffset + 1;
            foreach (var offset in _messageDict.Keys)
            {
                if (offset < currentMinQueueOffset)
                {
                    currentMinQueueOffset = offset;
                }
            }
            return currentMinQueueOffset - 1;
        }
    }
}
