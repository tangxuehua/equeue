using System.Collections.Generic;

namespace EQueue.Infrastructure
{
    public interface INameServerService
    {
        IEnumerable<string> FindConsumerClients(string consumerGroup);
    }
}
