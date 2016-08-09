using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumingMessage
    {
        public QueueMessage Message { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }
        public bool IsIgnored { get; set; }

        public ConsumingMessage(QueueMessage message, ProcessQueue processQueue)
        {
            Message = message;
            ProcessQueue = processQueue;
        }
    }
}
