using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Broker
{
    public interface IMessageQueueSelector
    {
        ConsumeQueue SelectQueue(IList<ConsumeQueue> totalQueues, Message message, object arg);
    }
}
