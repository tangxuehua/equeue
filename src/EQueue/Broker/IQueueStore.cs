using System.Collections.Generic;

namespace EQueue.Broker
{
    public interface IQueueStore
    {
        IEnumerable<Queue> LoadAllQueues();
        void CreateQueues(IEnumerable<Queue> queues);
        Queue GetQueue(string topic, int queueId);
        void DeleteQueue(Queue queue);
        void UpdateQueue(Queue queue);
    }
}
