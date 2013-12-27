using System.Collections.Concurrent;
using System.Threading;
using EQueue.Common;

namespace EQueue.Broker
{
    public class InMemoryMessageStore : IMessageStore
    {
        private ConcurrentDictionary<long, MessageData> _queueCurrentOffsetDict = new ConcurrentDictionary<long, MessageData>();
        private long _currentOffset = -1;

        public MessageStoreResult StoreMessage(Message message, int queueId, long queueOffset)
        {
            var offset = GetNextOffset();
            var messageId = ObjectId.GenerateNewId().ToString();
            _queueCurrentOffsetDict[offset] = new MessageData(messageId, message, queueId, queueOffset);
            return new MessageStoreResult(messageId, offset, queueId, queueOffset);
        }

        private long GetNextOffset()
        {
            return Interlocked.Increment(ref _currentOffset);
        }
        private class MessageData
        {
            public Message Message { get; private set; }
            public string MessageId { get; private set; }
            public int QueueId { get; private set; }
            public long QueueOffset { get; private set; }

            public MessageData(string messageId, Message message, int queueId, long queueOffset)
            {
                MessageId = messageId;
                Message = message;
                QueueId = queueId;
                QueueOffset = queueOffset;
            }
        }
    }
}
