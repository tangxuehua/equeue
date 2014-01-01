using System;
using System.Collections.Generic;
using EQueue.Common;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class ConsumeQueueHashSelector : IConsumeQueueSelector
    {
        public ConsumeQueue SelectQueue(IList<ConsumeQueue> totalQueues, Message message, string arg)
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
