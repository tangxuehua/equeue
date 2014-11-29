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
        public string RoutingKey { get; private set; }

        public QueueMessage(string topic, int code, byte[] body, long messageOffset, int queueId, long queueOffset, DateTime storedTime, string routingKey)
            : base(topic, code, body)
        {
            MessageOffset = messageOffset;
            QueueId = queueId;
            QueueOffset = queueOffset;
            StoredTime = storedTime;
            RoutingKey = routingKey;
        }
    }
}
