using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class PullResult
    {
        public PullStatus PullStatus { get; set; }
        public IEnumerable<QueueMessage> Messages { get; set; }
    }
}
