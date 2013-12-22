using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public interface IConsumer
    {
        string GroupName { get; }
        MessageModel MessageModel { get; }
        IEnumerable<string> SubscriptionTopics { get; }
        void PullMessage(PullRequest pullRequest);
        bool IsSubscribeTopicNeedUpdate(string topic);
        void UpdateTopicSubscribeInfo(string topic, IEnumerable<MessageQueue> messageQueues);
        void Rebalance();
        void PersistOffset();
    }
}
