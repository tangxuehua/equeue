using EQueue.Protocols;

namespace EQueue.Remoting.Responses
{
    public class SendMessageResponse
    {
        public string MessageId { get; private set; }
        public long MessageOffset { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public long QueueOffset { get; private set; }

        public SendMessageResponse(string messageId, long messageOffset, MessageQueue messageQueue, long queueOffset)
        {
            MessageId = messageId;
            MessageOffset = messageOffset;
            MessageQueue = messageQueue;
            QueueOffset = queueOffset;
        }
    }
}
