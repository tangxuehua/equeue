using System.Collections.Generic;

namespace EQueue.Broker
{
    public interface IQueueStore
    {
        IEnumerable<Queue> LoadAllQueues();
        bool IsQueueExist(string topic, int queueId);
        Queue GetQueue(string topic, int queueId);
        void CreateQueue(Queue queue);
        void DeleteQueue(Queue queue);
        void UpdateQueue(Queue queue);
    }
}
