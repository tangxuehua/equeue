using System;
using System.Collections.Generic;
using System.Threading;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueAverageSelector : IQueueSelector
    {
        private long _index;

        public int SelectQueueId(IList<int> availableQueueIds, Message message, string routingKey)
        {
            if (availableQueueIds.Count == 0)
            {
                throw new Exception(string.Format("No available queue for topic [{0}].", message.Topic));
            }
            return availableQueueIds[(int)(Interlocked.Increment(ref _index) % availableQueueIds.Count)];
        }
    }
}
