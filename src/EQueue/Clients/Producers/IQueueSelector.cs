using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public interface IQueueSelector
    {
        int SelectQueueId(int totalQueueCount, Message message, object arg);
    }
}
