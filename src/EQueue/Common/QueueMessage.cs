using System;

namespace EQueue.Common
{
    [Serializable]
    public class QueueMessage : Message
    {
        public Guid Id { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public QueueMessage(Guid id, string topic, byte[] body, int queueId, long queueOffset) : base(topic, body)
        {
            Id = id;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }
    }
}
