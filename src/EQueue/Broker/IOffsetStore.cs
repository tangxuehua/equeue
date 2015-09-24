using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IOffsetStore
    {
        void Start();
        void Shutdown();
        int GetConsumerGroupCount();
        long GetQueueOffset(string topic, int queueId, string group);
        long GetMinConsumedOffset(string topic, int queueId);
        void UpdateQueueOffset(string topic, int queueId, long offset, string group);
        IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic);
    }
}
