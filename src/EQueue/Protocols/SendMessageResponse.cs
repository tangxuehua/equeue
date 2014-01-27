using System;
using EQueue.Protocols;

namespace EQueue.Protocols
{
    [Serializable]
    public class SendMessageResponse
    {
        public long MessageOffset { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public long QueueOffset { get; private set; }

        public SendMessageResponse(long messageOffset, MessageQueue messageQueue, long queueOffset)
        {
            MessageOffset = messageOffset;
            MessageQueue = messageQueue;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[MessageOffset={0}, MessageQueue={1}, QueueOffset={2}]", MessageOffset, MessageQueue, QueueOffset);
        }
    }
}
