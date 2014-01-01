using System;
using EQueue.Common;

namespace EQueue.Clients.Producers
{
    public class SendResult
    {
        public SendStatus SendStatus { get; private set; }
        public string MessageId { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public long QueueOffset { get; private set; }
        public long MessageOffset { get; private set; }

        public SendResult(SendStatus sendStatus, string messageId, long messageOffset, string topic, int queueId, long queueOffset)
        {
            SendStatus = sendStatus;
            MessageId = messageId;
            MessageOffset = messageOffset;
            MessageQueue = new MessageQueue(topic, queueId);
            QueueOffset = queueOffset;
        }
    }
}
