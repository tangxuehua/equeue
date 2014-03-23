using System.Threading;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueAverageSelector : IQueueSelector
    {
        private long _index;

        public int SelectQueueId(int totalQueueCount, Message message, object arg)
        {
            return (int)(Interlocked.Increment(ref _index) % totalQueueCount);
        }
    }
}
