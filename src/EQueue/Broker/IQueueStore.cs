using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Broker
{
    public interface IQueueStore
    {
        void Load();
        void Start();
        void Shutdown();
        IEnumerable<string> GetAllTopics();
        Queue GetQueue(string topic, int queueId);
        int GetAllQueueCount();
        IEnumerable<Queue> GetAllQueues();
        IList<TopicQueueInfo> GetTopicQueueInfoList(string topic = null);
        long GetTotalUnConusmedMessageCount();
        bool IsTopicExist(string topic);
        bool IsQueueExist(QueueKey queueKey);
        bool IsQueueExist(string topic, int queueId);
        long GetQueueCurrentOffset(string topic, int queueId);
        long GetQueueMinOffset(string topic, int queueId);
        void AddQueue(string topic);
        void DeleteQueue(string topic, int queueId);
        void SetProducerVisible(string topic, int queueId, bool visible);
        void SetConsumerVisible(string topic, int queueId, bool visible);
        IEnumerable<int> CreateTopic(string topic, int? initialQueueCount);
        void DeleteTopic(string topic);
        IEnumerable<Queue> GetQueues(string topic, bool autoCreate = false);
    }
}
