using System;
using System.IO;
using System.Text;

namespace EQueue.Broker.Storage
{
    public class MessageLogRecord : LogRecord, ILogRecord
    {
        public string Topic { get; private set; }
        public int Code { get; private set; }
        public byte[] Body { get; private set; }
        public string MessageId { get; private set; }
        public string MessageKey { get; private set; }
        public long MessageOffset { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }
        public string RoutingKey { get; private set; }
        public DateTime CreatedTime { get; private set; }
        public DateTime StoredTime { get; internal set; }

        public MessageLogRecord()
            : base((byte)LogRecordType.Message)
        { }
        public MessageLogRecord(
            byte version,
            long logPosition,
            string topic,
            int code,
            string messageId,
            string messageKey,
            byte[] body,
            long messageOffset,
            int queueId,
            long queueOffset,
            string routingKey,
            DateTime createdTime,
            DateTime storedTime)
            : base((byte)LogRecordType.Message, version, logPosition)
        {
            Topic = topic;
            RoutingKey = routingKey;
            MessageId = messageId;
            MessageKey = messageKey;
            MessageOffset = messageOffset;
            Code = code;
            Body = body;
            QueueId = queueId;
            QueueOffset = queueOffset;
            CreatedTime = createdTime;
            StoredTime = storedTime;
        }

        public override void WriteTo(BinaryWriter writer)
        {
            base.WriteTo(writer);

            //topic
            var topicBytes = Encoding.UTF8.GetBytes(Topic);
            writer.Write(topicBytes.Length);
            writer.Write(topicBytes);

            //routingKey
            var routingKeyBytes = Encoding.UTF8.GetBytes(RoutingKey);
            writer.Write(routingKeyBytes.Length);
            writer.Write(routingKeyBytes);

            //messageId
            var messageIdBytes = Encoding.UTF8.GetBytes(MessageId);
            writer.Write(messageIdBytes.Length);
            writer.Write(messageIdBytes);

            //messageKey
            var messageKeyBytes = Encoding.UTF8.GetBytes(MessageKey);
            writer.Write(messageKeyBytes.Length);
            writer.Write(messageKeyBytes);

            //messageOffset
            writer.Write(MessageOffset);

            //code
            writer.Write(Code);

            //body
            writer.Write(Body.Length);
            writer.Write(Body);

            //queueId
            writer.Write(QueueId);

            //queueOffset
            writer.Write(QueueOffset);

            //createdTime
            writer.Write(CreatedTime.Ticks);

            //storedTime
            writer.Write(StoredTime.Ticks);

        }
        public override void ParseFrom(BinaryReader reader)
        {
            base.ParseFrom(reader);

            //topic
            Topic = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //routingKey
            RoutingKey = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //messageId
            MessageId = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //messageKey
            MessageKey = Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32()));

            //messageOffset
            MessageOffset = reader.ReadInt64();

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
    }
}
