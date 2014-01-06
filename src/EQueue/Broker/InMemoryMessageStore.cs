using System;
using System.Collections.Concurrent;
using System.Threading;
using EQueue.Infrastructure;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private ConcurrentDictionary<long, QueueMessage> _queueCurrentOffsetDict = new ConcurrentDictionary<long, QueueMessage>();
        private long _currentOffset = -1;

        public MessageStoreResult StoreMessage(Message message, int queueId, long queueOffset)
        {
            var offset = GetNextOffset();
            _queueCurrentOffsetDict[offset] = new QueueMessage(message.Topic, message.Body, queueId, queueOffset, DateTime.Now);
            return new MessageStoreResult(offset, queueId, queueOffset);
        }
        public QueueMessage GetMessage(long offset)
        {
            QueueMessage queueMessage;
            if (_queueCurrentOffsetDict.TryGetValue(offset, out queueMessage))
            {
                return queueMessage;
            }
            return null;
        }
        public void Recover() { }

        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
    }
}
