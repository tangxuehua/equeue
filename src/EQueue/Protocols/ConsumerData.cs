using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class ConsumerData
    {
        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public MessageModel MessageModel { get; private set; }
        public IEnumerable<string> SubscriptionTopics { get; private set; }

        public ConsumerData(string consumerId, string groupName, MessageModel messageModel, IEnumerable<string> subscriptionTopics)
        {
            ConsumerId = consumerId;
            GroupName = groupName;
            MessageModel = messageModel;
            SubscriptionTopics = subscriptionTopics;
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId:{0}, GroupName:{1}, MessageModel:{2}, SubscriptionTopics:{3}]", ConsumerId, GroupName, MessageModel, string.Join("|", SubscriptionTopics));
        }
    }
}
