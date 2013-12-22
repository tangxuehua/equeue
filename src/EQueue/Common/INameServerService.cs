using System.Collections.Generic;

namespace EQueue.Common
{
    public interface INameServerService
    {
        IEnumerable<string> FindConsumerClients(string consumerGroup);
    }
}
