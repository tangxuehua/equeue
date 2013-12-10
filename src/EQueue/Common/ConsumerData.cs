using System;
using System.Collections.Generic;

namespace EQueue.Common
{
    [Serializable]
    public class ConsumerData
    {
        public string GroupName { get; private set; }
        public MessageModel MessageModel { get; private set; }
        public IEnumerable<string> SubscriptionTopics { get; private set; }

        public ConsumerData(string groupName, MessageModel messageModel, IEnumerable<string> subscriptionTopics)
        {
            GroupName = groupName;
            MessageModel = messageModel;
            SubscriptionTopics = subscriptionTopics;
        }

        public override string ToString()
        {
            return string.Format("[GroupName:{0}, MessageModel:{1}, SubscriptionTopics:{2}]", GroupName, MessageModel, string.Join("|", SubscriptionTopics));
        }
    }
}
