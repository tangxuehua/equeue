using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class QueueHashSelector : IQueueSelector
    {
        public Queue SelectQueue(IList<Queue> totalQueues, Message message, string arg)
        {
            var value = arg.GetHashCode();
            if (value < 0)
            {
                value = Math.Abs(value);
            }
            value = value % totalQueues.Count;
            return totalQueues[value];
        }
    }
}
