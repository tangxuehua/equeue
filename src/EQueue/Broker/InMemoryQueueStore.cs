using System.Collections.Concurrent;
using System.Collections.Generic;
using ECommon.Extensions;

namespace EQueue.Broker
{
    public class InMemoryQueueStore : IQueueStore
    {
        private readonly ConcurrentDictionary<string, Queue> _queueDict = new ConcurrentDictionary<string, Queue>();

        public IEnumerable<Queue> LoadAllQueues()
        {
            return _queueDict.Values;
        }
        public bool IsQueueExist(string topic, int queueId)
        {
            return _queueDict.ContainsKey(CreateQueueKey(topic, queueId));
        }
        public Queue GetQueue(string topic, int queueId)
        {
            var key = CreateQueueKey(topic, queueId);
            Queue queue;
            if (_queueDict.TryGetValue(key, out queue))
            {
                return queue;
            }
            return null;
        }
        public void CreateQueue(Queue queue)
        {
            _queueDict.TryAdd(CreateQueueKey(queue.Topic, queue.QueueId), queue);
        }
        public void DeleteQueue(Queue queue)
        {
            _queueDict.Remove(CreateQueueKey(queue.Topic, queue.QueueId));
        }
        public void UpdateQueue(Queue queue)
        {
            _queueDict[CreateQueueKey(queue.Topic, queue.QueueId)] = queue;
        }

        private static string CreateQueueKey(string topic, int queueId)
        {
            return string.Format("{0}-{1}", topic, queueId);
        }
    }
}
