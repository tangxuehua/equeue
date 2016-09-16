using System;
using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Protocols.NameServers
{
    [Serializable]
    public class BrokerTopicConsumeInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<TopicConsumeInfo> TopicConsumeInfoList { get; set; }

        public BrokerTopicConsumeInfo()
        {
            TopicConsumeInfoList = new List<TopicConsumeInfo>();
        }
    }
}
