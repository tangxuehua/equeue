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

        public override string ToString()
        {
            return string.Format("[MessageId={0}, MessageOffset={1}, MessageQueue={2}, QueueOffset={3}]", MessageId, MessageOffset, MessageQueue, QueueOffset);
        }
    }
}
