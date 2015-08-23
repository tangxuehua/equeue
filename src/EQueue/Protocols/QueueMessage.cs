using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        public string MessageId { get; private set; }
        public long MessageOffset { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }
        public DateTime ArrivedTime { get; private set; }
        public DateTime StoredTime { get; internal set; }
        public string RoutingKey { get; private set; }

        public QueueMessage(string messageId, string topic, int code, string key, byte[] body, long messageOffset, int queueId, long queueOffset, DateTime createdTime, DateTime arrivedTime, DateTime storedTime, string routingKey)
            : base(topic, code, key, body, createdTime)
        {
            MessageId = messageId;
            MessageOffset = messageOffset;
            QueueId = queueId;
            QueueOffset = queueOffset;
            ArrivedTime = arrivedTime;
            StoredTime = storedTime;
            RoutingKey = routingKey;
        }
    }
}
