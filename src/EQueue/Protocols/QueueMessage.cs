using System;
using System.IO;
using System.Text;
using EQueue.Utils;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueueMessage : Message
    {
        public long LogPosition { get; set; }
        public string MessageId { get; set; }
        public int QueueId { get; set; }
        public long QueueOffset { get; set; }
        public DateTime StoredTime { get; set; }
        public string RoutingKey { get; set; }

        public QueueMessage() { }
        public QueueMessage(string messageId, string topic, int code, string key, byte[] body, int queueId, long queueOffset, DateTime createdTime, DateTime storedTime, string routingKey)
            : base(topic, code, key, body, createdTime)
        {
            MessageId = messageId;
            QueueId = queueId;
            QueueOffset = queueOffset;
            StoredTime = storedTime;
            RoutingKey = routingKey;
        }

        public virtual void ReadFrom(byte[] recordBuffer)
        {
            var srcOffset = 0;

            LogPosition = MessageUtils.DecodeLong(recordBuffer, srcOffset, out srcOffset);
            MessageId = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Topic = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            RoutingKey = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Key = MessageUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Code = MessageUtils.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            Body = MessageUtils.DecodeBytes(recordBuffer, srcOffset, out srcOffset);
            QueueId = MessageUtils.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            QueueOffset = MessageUtils.DecodeLong(recordBuffer, srcOffset, out srcOffset);
            CreatedTime = MessageUtils.DecodeDateTime(recordBuffer, srcOffset, out srcOffset);
            StoredTime = MessageUtils.DecodeDateTime(recordBuffer, srcOffset, out srcOffset);
        }
        public bool IsValid()
        {
            return !string.IsNullOrEmpty(MessageId);
        }
    }
}
