using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Client.Consumer
{
    public interface IConsumer
    {
        string GroupName { get; }
        MessageModel MessageModel { get; }
        IEnumerable<string> SubscriptionTopics { get; }
        void Start();
        void Shutdown();
        void DoRebalance();
        void PersistOffset();
        void PullMessage(PullRequest pullRequest);
        void UpdateTopicSubscribeInfo(string topic, IEnumerable<MessageQueue> messageQueues);
        bool IsSubscribeTopicNeedUpdate(string topic);
    }
}
