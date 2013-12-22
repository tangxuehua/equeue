using System;
using System.Collections.Generic;
using System.Linq;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public class AverageAllocateMessageQueueStrategy : IAllocateMessageQueueStrategy
    {
        public IEnumerable<MessageQueue> Allocate(string currentConsumerClientId, IList<MessageQueue> totalMessageQueues, IList<string> totalConsumerClientIds)
        {
            if (string.IsNullOrEmpty(currentConsumerClientId))
            {
                throw new ArgumentException("currentConsumerClientId is empty");
            }
            if (totalMessageQueues == null || totalMessageQueues.Count() < 1)
            {
                throw new ArgumentException("totalMessageQueues is null or size < 1");
            }
            if (totalConsumerClientIds == null || totalConsumerClientIds.Count() < 1)
            {
                throw new ArgumentException("totalConsumerClientIds is null or size < 1");
            }

            var result = new List<MessageQueue>();

            if (!totalConsumerClientIds.Contains(currentConsumerClientId))
            {
                return result;
            }

            var index = totalConsumerClientIds.IndexOf(currentConsumerClientId);
            var totalMessageQueueCount = totalMessageQueues.Count;
            var totalConsumerClientIdCount = totalConsumerClientIds.Count;
            var mod = totalMessageQueues.Count() % totalConsumerClientIdCount;
            var averageSize = totalMessageQueueCount <= totalConsumerClientIdCount ? 1 : (mod > 0 && index < mod ? totalMessageQueueCount / totalConsumerClientIdCount + 1 : totalMessageQueueCount / totalConsumerClientIdCount);
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
