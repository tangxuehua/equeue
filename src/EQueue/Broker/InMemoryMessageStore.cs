using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private ConcurrentDictionary<long, QueueMessage> _messageDict = new ConcurrentDictionary<long, QueueMessage>();
        private long _currentOffset = -1;

        public IEnumerable<QueueMessage> Messages { get { return _messageDict.Values; } }

        public QueueMessage StoreMessage(int queueId, long queueOffset, Message message)
        {
            var nextOffset = GetNextOffset();
            var queueMessage = new QueueMessage(message.Topic, message.Body, nextOffset, queueId, queueOffset, DateTime.Now);
            _messageDict[nextOffset] = queueMessage;
            return queueMessage;
        }

        public void Recover() { }
        public void Start() { }
        public void Shutdown() { }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_messageDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public void RemoveMessage(long messageOffset)
        {
            QueueMessage queueMessage;
            _messageDict.TryRemove(messageOffset, out queueMessage);
        }
        public void DeleteMessages(string topic, int queueId, long maxQueueOffset)
        {
        }

        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
    }
}
