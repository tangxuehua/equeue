using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        public long MessageOffset { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }
        public DateTime StoredTime { get; private set; }

        public QueueMessage(string topic, byte[] body, long messageOffset, int queueId, long queueOffset, DateTime storedTime) : base(topic, body)
        {
            MessageOffset = messageOffset;
            QueueId = queueId;
            QueueOffset = queueOffset;
            StoredTime = storedTime;
        }
    }
}
