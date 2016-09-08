using System.Collections.Generic;
using System.Threading;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueAverageSelector : IQueueSelector
    {
        private long _index;

        public MessageQueue SelectMessageQueue(IList<MessageQueue> availableMessageQueues, Message message, string routingKey)
        {
            if (availableMessageQueues.Count == 0)
            {
                return null;
            }
            return availableMessageQueues[(int)(Interlocked.Increment(ref _index) % availableMessageQueues.Count)];
        }
    }
}
