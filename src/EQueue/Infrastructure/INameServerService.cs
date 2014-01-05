using System.Collections.Generic;

namespace EQueue.Infrastructure
{
    public interface INameServerService
    {
        IEnumerable<string> FindConsumers(string consumerGroup);
    }
}
