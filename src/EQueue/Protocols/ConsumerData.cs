using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class ConsumerData
    {
        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public IEnumerable<string> SubscriptionTopics { get; private set; }
        public IEnumerable<string> ConsumingQueues { get; private set; }

        public ConsumerData(string consumerId, string groupName, IEnumerable<string> subscriptionTopics, IEnumerable<string> consumingQueues)
        {
            ConsumerId = consumerId;
            GroupName = groupName;
            SubscriptionTopics = subscriptionTopics;
            ConsumingQueues = consumingQueues;
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId:{0}, GroupName:{1}, SubscriptionTopics:{2}, ConsumingQueues:{3}]", ConsumerId, GroupName, string.Join("|", SubscriptionTopics), string.Join("|", ConsumingQueues));
        }
    }
}
