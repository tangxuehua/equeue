using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public interface IQueueSelector
    {
        MessageQueue SelectMessageQueue(IList<MessageQueue> availableMessageQueues, Message message, string routingKey);
    }
}
