using System.Collections.Generic;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumingMessage
    {
        public QueueMessage Message { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }

        public ConsumingMessage(QueueMessage message, ProcessQueue processQueue)
        {
            Message = message;
            ProcessQueue = processQueue;
        }
    }
}
