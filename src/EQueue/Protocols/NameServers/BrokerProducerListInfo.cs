using System;
using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.Protocols.NameServers
{
    [Serializable]
    public class BrokerProducerListInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<string> ProducerList { get; set; }

        public BrokerProducerListInfo()
        {
            ProducerList = new List<string>();
        }
    }
}
