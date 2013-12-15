using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public interface IAllocateMessageQueueStrategy
    {
        IEnumerable<MessageQueue> Allocate(string currentConsumerId, IList<MessageQueue> totalMessageQueues, IList<string> totalConsumerIds);
    }
}
