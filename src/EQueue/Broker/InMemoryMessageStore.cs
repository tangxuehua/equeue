using System.Collections.Concurrent;
using System.Threading;
using EQueue.Common;
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
            var messageId = ObjectId.GenerateNewId().ToString();
            _queueCurrentOffsetDict[offset] = new QueueMessage(messageId, message.Topic, message.Body, queueId, queueOffset);
            return new MessageStoreResult(messageId, offset, queueId, queueOffset);
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
