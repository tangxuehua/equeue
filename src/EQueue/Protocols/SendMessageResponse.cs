using System;
using EQueue.Protocols;

namespace EQueue.Protocols
{
    [Serializable]
    public class SendMessageResponse
    {
        public string MessageKey { get; private set; }
        public string MessageId { get; private set; }
        public long MessageOffset { get; private set; }
        public int MessageCode { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public SendMessageResponse(string messageKey, string messageId, long messageOffset, int messageCode, string topic, int queueId, long queueOffset)
        {
            MessageKey = messageKey;
            MessageId = messageId;
            MessageOffset = messageOffset;
            MessageCode = messageCode;
            Topic = topic;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[MessageKey:{0},MessageId:{1},MessageOffset:{2},MessageCode:{3},Topic:{4},QueueId:{5},QueueOffset{6}]",
                MessageKey,
                MessageId,
                MessageOffset,
                MessageCode,
                Topic,
                QueueId,
                QueueOffset);
        }
    }
}
