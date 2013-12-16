using System;
using System.Collections.Generic;
using EQueue.Common;

namespace EQueue.Clients.Consumers
{
    public interface IRebalanceService
    {
        IEnumerable<string> SubscriptionTopics { get; }
        IEnumerable<MessageQueue> ProcessingMessageQueues { get; }

        void Rebalance();
        void RegisterSubscriptionTopic(string topic);
        void UpdateTopicSubscribeInfo(string topic, IEnumerable<MessageQueue> messageQueues);
    }
}
