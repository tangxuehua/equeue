using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Broker
{
    public interface IConsumeQueueSelector
    {
        ConsumeQueue SelectQueue(IList<ConsumeQueue> totalQueues, Message message, string arg);
    }
}
