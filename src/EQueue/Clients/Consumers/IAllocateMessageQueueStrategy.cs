using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public interface IAllocateMessageQueueStrategy
    {
        IEnumerable<MessageQueue> Allocate(string currentConsumerClientId, IList<MessageQueue> totalMessageQueues, IList<string> totalConsumerClientIds);
    }
}
