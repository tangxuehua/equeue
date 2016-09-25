using System;

namespace EQueue.Protocols.Brokers
{
    [Serializable]
    public class BrokerStatusInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public long TotalSendThroughput { get; set; }
        public long TotalConsumeThroughput { get; set; }
        public long TotalUnConsumedMessageCount { get; set; }
    }
}
