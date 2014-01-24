using System;
using EQueue.Clients.Producers;
using EQueue.Protocols;

namespace EQueue.Clients.Producers
{
    public class QueueHashSelector : IQueueSelector
    {
        public int SelectQueueId(int totalQueueCount, Message message, object arg)
        {
            var value = arg.GetHashCode();
            if (value < 0)
            {
                value = Math.Abs(value);
            }
            return value % totalQueueCount;
        }
    }
}
