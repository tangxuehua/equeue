using System;
using System.Collections.Generic;
using System.Linq;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class AverageAllocateMessageQueueStrategy : IAllocateMessageQueueStrategy
    {
        public IEnumerable<MessageQueue> Allocate(string currentConsumerId, IList<MessageQueue> totalMessageQueues, IList<string> totalConsumerIds)
        {
            var result = new List<MessageQueue>();

            if (!totalConsumerIds.Contains(currentConsumerId))
            {
                return result;
            }

            var index = totalConsumerIds.IndexOf(currentConsumerId);
            var totalMessageQueueCount = totalMessageQueues.Count;
            var totalConsumerCount = totalConsumerIds.Count;
            var mod = totalMessageQueues.Count() % totalConsumerCount;
            var averageSize = totalMessageQueueCount <= totalConsumerCount ? 1 : (mod > 0 && index < mod ? totalMessageQueueCount / totalConsumerCount + 1 : totalMessageQueueCount / totalConsumerCount);
            var startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
            var range = Math.Min(averageSize, totalMessageQueueCount - startIndex);

            for (var i = 0; i < range; i++)
            {
                result.Add(totalMessageQueues[(startIndex + i) % totalMessageQueueCount]);
            }

            return result;
        }
    }
}
