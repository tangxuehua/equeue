using System;
using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Protocols.NameServers
{
    [Serializable]
    public class BrokerConsumerListInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<ConsumerInfo> ConsumerList { get; set; }

        public BrokerConsumerListInfo()
        {
            ConsumerList = new List<ConsumerInfo>();
        }
    }
}
