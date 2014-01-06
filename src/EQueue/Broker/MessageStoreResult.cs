using System;

namespace EQueue.Broker
{
    public class MessageStoreResult
    {
        public long MessageOffset { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public MessageStoreResult(long messageOffset, int queueId, long queueOffset)
        {
            MessageOffset = messageOffset;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }
    }
}
