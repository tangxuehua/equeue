using System;
using EQueue.Protocols;

namespace EQueue.Protocols
{
    public class MessageStoreResult
    {
        public string MessageId { get; private set; }
        public int Code { get; private set; }
        public string Topic { get; private set; }
        public string Tag { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public MessageStoreResult(string messageId, int code, string topic, int queueId, long queueOffset, string tag = null)
        {
            MessageId = messageId;
            Code = code;
            Topic = topic;
            Tag = tag;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[MessageId:{0}, Code:{1}, Topic:{2}, QueueId:{3}, QueueOffset:{4}, Tag:{5}]",
                MessageId,
                Code,
                Topic,
                QueueId,
                QueueOffset,
                Tag);
        }
    }
}
