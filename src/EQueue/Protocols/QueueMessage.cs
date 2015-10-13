using System;
using System.IO;
using System.Text;

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

        public virtual void ReadFrom(int length, BinaryReader reader)
        {
            //logPosition
            LogPosition = reader.ReadInt64();

            //messageId
            MessageId = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //topic
            Topic = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //routingKey
            RoutingKey = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //key
            Key = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //code
            Code = reader.ReadInt32();

            //body
            Body = reader.ReadBytes(reader.ReadInt32());

            //queueId
            QueueId = reader.ReadInt32();

            //queueOffset
            QueueOffset = reader.ReadInt64();

            //createdTime
            CreatedTime = new DateTime(reader.ReadInt64());

            //storedTime
            StoredTime = new DateTime(reader.ReadInt64());
        }
        public bool IsValid()
        {
            return !string.IsNullOrEmpty(MessageId);
        }
    }
}
