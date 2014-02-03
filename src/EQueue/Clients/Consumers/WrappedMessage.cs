using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class WrappedMessage
    {
        public MessageQueue MessageQueue { get; private set; }
        public QueueMessage QueueMessage { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }

        public WrappedMessage(MessageQueue messageQueue, QueueMessage queueMessage, ProcessQueue processQueue)
        {
            MessageQueue = messageQueue;
            QueueMessage = queueMessage;
            ProcessQueue = processQueue;
        }
    }
}
