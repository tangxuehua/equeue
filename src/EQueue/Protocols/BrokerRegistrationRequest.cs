using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class BrokerRegistrationRequest
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IDictionary<string, IList<QueueInfo>> QueueInfoDict { get; set; }
    }
    [Serializable]
    public class QueueInfo
    {
        public int QueueId { get; set; }
        public bool ProducerVisible { get; set; }
        public bool ConsumerVisible { get; set; }
    }
}
