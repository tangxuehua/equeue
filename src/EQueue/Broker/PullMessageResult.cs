using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class PullMessageResult
    {
        public PullStatus Status { get; set; }
        public long NextBeginOffset { get; set; }
        public IEnumerable<byte[]> Messages { get; set; }
    }
}
