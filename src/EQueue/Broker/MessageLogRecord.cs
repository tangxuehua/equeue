using System;
using System.IO;
using System.Text;
using EQueue.Broker.Storage.LogRecords;
using EQueue.Protocols;

namespace EQueue.Broker.Storage
{
    [Serializable]
    public class MessageLogRecord : QueueMessage, ILogRecord
    {
        public MessageLogRecord() { }
        public MessageLogRecord(
            string topic,
            int code,
            string key,
            byte[] body,
            int queueId,
            long queueOffset,
            string routingKey,
            DateTime createdTime,
            DateTime storedTime)
            : base(null, topic, code, key, body, queueId, queueOffset, createdTime, storedTime, routingKey) { }

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            LogPosition = logPosition;
            MessageId = CreateMessageId(logPosition);

            //logPosition
            writer.Write(LogPosition);

            //messageId
            var messageIdBytes = Encoding.UTF8.GetBytes(MessageId);
            writer.Write(messageIdBytes.Length);
            writer.Write(messageIdBytes);

            //topic
            var topicBytes = Encoding.UTF8.GetBytes(Topic);
            writer.Write(topicBytes.Length);
            writer.Write(topicBytes);

            //routingKey
            var routingKeyBytes = Encoding.UTF8.GetBytes(RoutingKey);
            writer.Write(routingKeyBytes.Length);
            writer.Write(routingKeyBytes);

            //key
            var keyBytes = Encoding.UTF8.GetBytes(Key);
            writer.Write(keyBytes.Length);
            writer.Write(keyBytes);

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

        private static string CreateMessageId(long messagePosition)
        {
            //TODO，还要结合当前的Broker的IP作为MessageId的一部分
            return messagePosition.ToString();
        }
    }
}
