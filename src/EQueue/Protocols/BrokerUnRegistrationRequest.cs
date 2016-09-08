using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class BrokerUnRegistrationRequest
    {
        public BrokerInfo BrokerInfo { get; set; }
    }
}
