using System;
using EQueue.Protocols;

namespace EQueue.Protocols
{
    public class MessageStoreResult
    {
        public string MessageId { get; private set; }
        public int Code { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public MessageStoreResult(string messageId, int code, string topic, int queueId, long queueOffset)
        {
            MessageId = messageId;
            Code = code;
            Topic = topic;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[MessageId: {0}, Code: {1}, Topic: {2}, QueueId: {3}, QueueOffset: {4}]",
                MessageId,
                Code,
                Topic,
                QueueId,
                QueueOffset);
        }
    }
}
