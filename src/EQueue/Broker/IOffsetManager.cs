using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IOffsetManager
    {
        void Recover();
        void Start();
        void Shutdown();
        void UpdateQueueOffset(string topic, int queueId, long offset, string group);
        long GetQueueOffset(string topic, int queueId, string group);
        long GetMinOffset(string topic, int queueId);
        void RemoveQueueOffset(string topic, int queueId);
        void RemoveQueueOffset(string consumerGroup, string topic, int queueId);
        IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic);
    }
}
