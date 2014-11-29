using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueHashSelector : IQueueSelector
    {
        public int SelectQueueId(IList<int> availableQueueIds, Message message, string routingKey)
        {
            if (availableQueueIds.Count == 0)
            {
                throw new Exception(string.Format("No available queue for topic [{0}].", message.Topic));
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
                return availableQueueIds[(int)(hash % availableQueueIds.Count)];
            }
        }
    }
}
