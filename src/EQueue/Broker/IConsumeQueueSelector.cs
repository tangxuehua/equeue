using System.Collections.Generic;
using EQueue.Common;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IConsumeQueueSelector
    {
        ConsumeQueue SelectQueue(IList<ConsumeQueue> totalQueues, Message message, string arg);
    }
}
