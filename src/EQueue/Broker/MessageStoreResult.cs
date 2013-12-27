using System;

namespace EQueue.Broker
{
    public class MessageStoreResult
    {
        public string MessageId { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public MessageStoreResult(string messageId, int queueId, long queueOffset)
        {
            MessageId = messageId;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }
    }
}
