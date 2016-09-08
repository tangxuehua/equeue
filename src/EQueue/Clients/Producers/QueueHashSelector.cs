using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueHashSelector : IQueueSelector
    {
        public MessageQueue SelectMessageQueue(IList<MessageQueue> availableMessageQueues, Message message, string routingKey)
        {
            if (availableMessageQueues.Count == 0)
            {
                return null;
            }
            unchecked
            {
                int hash = 23;
                foreach (char c in routingKey)
                {
                    hash = (hash << 5) - hash + c;
                }
                if (hash < 0)
                {
                    hash = Math.Abs(hash);
                }
                return availableMessageQueues[hash % availableMessageQueues.Count];
            }
        }
    }
}
