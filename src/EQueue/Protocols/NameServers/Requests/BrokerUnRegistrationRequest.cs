using System;
using EQueue.Protocols.Brokers;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class BrokerUnRegistrationRequest
    {
        public BrokerInfo BrokerInfo { get; set; }
    }
}
