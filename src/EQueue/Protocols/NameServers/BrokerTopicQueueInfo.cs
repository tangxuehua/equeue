using System;
using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Protocols.NameServers
{
    [Serializable]
    public class BrokerTopicQueueInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<TopicQueueInfo> TopicQueueInfoList { get; set; }

        public BrokerTopicQueueInfo()
        {
            TopicQueueInfoList = new List<TopicQueueInfo>();
        }
    }
}
