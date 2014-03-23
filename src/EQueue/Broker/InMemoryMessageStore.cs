using System;
using System.Collections.Concurrent;
using System.Threading;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private long _currentOffset = -1;

        public QueueMessage StoreMessage(Message message, int queueId, long queueOffset)
        {
            var offset = GetNextOffset();
            var queueMessage = new QueueMessage(message.Topic, message.Body, offset, queueId, queueOffset, DateTime.Now);
            _messageDict[offset] = queueMessage;
            return queueMessage;
        }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_messageDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public bool RemoveMessage(long offset)
        {
            QueueMessage queueMessage;
            return _messageDict.TryRemove(offset, out queueMessage);
        }
        public void Recover() { }

        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
    }
}
