using System;
using System.Collections.Generic;
using System.Linq;
using EQueue.Common;

namespace EQueue.Client.Consumer
{
    public class AverageAllocateMessageQueueStrategy : IAllocateMessageQueueStrategy
    {
        public IEnumerable<MessageQueue> Allocate(string currentConsumerId, IList<MessageQueue> totalMessageQueues, IList<string> totalConsumerIds)
        {
            if (string.IsNullOrEmpty(currentConsumerId))
            {
                throw new ArgumentException("currentConsumerId is empty");
            }
            if (totalMessageQueues == null || totalMessageQueues.Count() < 1)
            {
                throw new ArgumentException("totalMessageQueues is null or size < 1");
            }
            if (totalConsumerIds == null || totalConsumerIds.Count() < 1)
            {
                throw new ArgumentException("totalConsumerIds is null or size < 1");
            }

            var result = new List<MessageQueue>();

            if (!totalConsumerIds.Contains(currentConsumerId))
            {
                return result;
            }

            var index = totalConsumerIds.IndexOf(currentConsumerId);
            var totalMessageQueueCount = totalMessageQueues.Count;
            var totalConsumerIdCount = totalConsumerIds.Count;
            var mod = totalMessageQueues.Count() % totalConsumerIdCount;
            var averageSize = totalMessageQueueCount <= totalConsumerIdCount ? 1 : (mod > 0 && index < mod ? totalMessageQueueCount / totalConsumerIdCount + 1 : totalMessageQueueCount / totalConsumerIdCount);
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
