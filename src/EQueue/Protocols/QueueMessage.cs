using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }
        public DateTime StoredTime { get; private set; }

        public QueueMessage(string topic, byte[] body, int queueId, long queueOffset, DateTime storedTime) : base(topic, body)
        {
            QueueId = queueId;
            QueueOffset = queueOffset;
            StoredTime = storedTime;
        }
    }
}
