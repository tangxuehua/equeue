using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        public string Id { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public QueueMessage(string id, string topic, byte[] body, int queueId, long queueOffset) : base(topic, body)
        {
            Id = id;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }
    }
}
