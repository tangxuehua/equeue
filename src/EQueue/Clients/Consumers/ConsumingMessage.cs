using System.Collections.Generic;
using EQueue.Broker.Storage;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class ConsumingMessage
    {
        public MessageLogRecord Message { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }

        public ConsumingMessage(MessageLogRecord message, ProcessQueue processQueue)
        {
            Message = message;
            ProcessQueue = processQueue;
        }
    }
}
