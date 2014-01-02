using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IQueueSelector
    {
        Queue SelectQueue(IList<Queue> totalQueues, Message message, string arg);
    }
}
