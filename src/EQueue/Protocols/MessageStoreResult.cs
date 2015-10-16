using System;
using EQueue.Protocols;

namespace EQueue.Protocols
{
    public class MessageStoreResult
    {
        public string MessageKey { get; private set; }
        public string MessageId { get; private set; }
        public int MessageCode { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public MessageStoreResult(string messageKey, string messageId, int messageCode, string topic, int queueId, long queueOffset)
        {
            MessageKey = messageKey;
            MessageId = messageId;
            MessageCode = messageCode;
            Topic = topic;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[MessageKey:{0},MessageId:{1},MessageCode:{2},Topic:{3},QueueId:{4},QueueOffset{5}]",
                MessageKey,
                MessageId,
                MessageCode,
                Topic,
                QueueId,
                QueueOffset);
        }
    }
}
