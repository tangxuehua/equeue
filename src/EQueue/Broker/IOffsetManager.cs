using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IOffsetManager
    {
        void Start();
        void Shutdown();
        int GetConsumerGroupCount();
        long GetQueueOffset(string topic, int queueId, string group);
        long GetMinOffset(string topic, int queueId);
        void UpdateQueueOffset(string topic, int queueId, long offset, string group);
        void DeleteQueueOffset(string topic, int queueId);
        void DeleteQueueOffset(string consumerGroup, string topic, int queueId);
        IEnumerable<QueueConsumedOffset> GetQueueConsumedOffsets();
        IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic);
    }
}
