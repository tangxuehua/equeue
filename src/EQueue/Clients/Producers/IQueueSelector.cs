using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public interface IQueueSelector
    {
        int SelectQueueId(IList<int> availableQueueIds, Message message, string routingKey);
    }
}
