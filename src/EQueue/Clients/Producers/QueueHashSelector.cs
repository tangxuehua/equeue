using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueHashSelector : IQueueSelector
    {
        public int SelectQueueId(IList<int> availableQueueIds, Message message, object arg)
        {
            if (availableQueueIds.Count == 0)
            {
                throw new Exception(string.Format("No available queue for topic [{0}].", message.Topic));
            }
            var value = arg.GetHashCode();
            if (value < 0)
            {
                value = Math.Abs(value);
            }
            return availableQueueIds[(int)(value % availableQueueIds.Count)];
        }
    }
}
