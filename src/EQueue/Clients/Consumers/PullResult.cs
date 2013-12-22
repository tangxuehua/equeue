using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public class PullResult
    {
        public PullStatus PullStatus { get; set; }
        public long NextBeginOffset { get; set; }
        public long MinOffset { get; set; }
        public long MaxOffset { get; set; }
        public IEnumerable<QueueMessage> Messages { get; set; }
    }
}
