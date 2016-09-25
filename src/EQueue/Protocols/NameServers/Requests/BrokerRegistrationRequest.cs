using System;
using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class BrokerRegistrationRequest
    {
        public BrokerInfo BrokerInfo { get; set; }
        public long TotalSendThroughput { get; set; }
        public long TotalConsumeThroughput { get; set; }
        public long TotalUnConsumedMessageCount { get; set; }
        public IList<TopicQueueInfo> TopicQueueInfoList { get; set; }
        public IList<TopicConsumeInfo> TopicConsumeInfoList { get; set; }
        public IList<string> ProducerList { get; set; }
        public IList<ConsumerInfo> ConsumerList { get; set; }
    }
}
