using System.Collections.Generic;
using EQueue.Broker.Storage;

namespace EQueue.Broker
{
    public interface IQueueService
    {
        void Start();
        void Shutdown();
        IEnumerable<string> GetAllTopics();
        Queue GetQueue(string topic, int queueId);
        int GetAllQueueCount();
        long GetAllQueueIndexCount();
        long GetAllQueueUnConusmedMessageCount();
        long GetQueueMinMessageOffset();
        bool IsQueueExist(string topic, int queueId);
        long GetQueueCurrentOffset(string topic, int queueId);
        long GetQueueMinOffset(string topic, int queueId);
        void AddQueue(string topic);
        void RemoveQueue(string topic, int queueId);
        void EnableQueue(string topic, int queueId);
        void DisableQueue(string topic, int queueId);
        void CreateTopic(string topic, int initialQueueCount);
        IEnumerable<Queue> QueryQueues(string topic);
        IEnumerable<Queue> FindQueues(string topic, QueueStatus? status = null);
        IEnumerable<Queue> GetOrCreateQueues(string topic, QueueStatus? status = null);
    }
}
