using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public interface IConsumeOffsetStore
    {
        void Start();
        void Shutdown();
        int GetConsumerGroupCount();
        long GetConsumeOffset(string topic, int queueId, string group);
        long GetMinConsumedOffset(string topic, int queueId);
        void UpdateConsumeOffset(string topic, int queueId, long offset, string group);
        IEnumerable<TopicConsumeInfo> QueryTopicConsumeInfos(string groupName, string topic);
    }
}
