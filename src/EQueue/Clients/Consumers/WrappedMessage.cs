using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class WrappedMessage
    {
        public QueueMessage QueueMessage { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }

        public WrappedMessage(QueueMessage queueMessage, MessageQueue messageQueue, ProcessQueue processQueue)
        {
            QueueMessage = queueMessage;
            MessageQueue = messageQueue;
            ProcessQueue = processQueue;
        }
    }
}
