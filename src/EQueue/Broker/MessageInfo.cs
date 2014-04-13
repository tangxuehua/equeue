using EQueue.Protocols;

namespace EQueue.Broker
{
    public class MessageInfo
    {
        public Queue Queue { get; private set; }
        public long QueueOffset { get; private set; }
        public Message Message { get; private set; }
        public QueueMessage QueueMessage { get; set; }

        public MessageInfo(Queue queue, long queueOffset, Message message)
        {
            Queue = queue;
            QueueOffset = queueOffset;
            Message = message;
        }
    }
}
