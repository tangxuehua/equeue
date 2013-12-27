using System;
using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Broker
{
    public class MessageQueueHashSelector : IMessageQueueSelector
    {
        public ConsumeQueue SelectQueue(IList<ConsumeQueue> totalQueues, Message message, object arg)
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
