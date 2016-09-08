using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class TopicRouteInfo
    {
        public BrokerInfo BrokerInfo { get; set; }
        public IList<int> QueueInfo { get; set; }

        public TopicRouteInfo()
        {
            QueueInfo = new List<int>();
        }

        public override string ToString()
        {
            return string.Format("[BrokerInfo: {0}, QueueInfo: {1}]", BrokerInfo, string.Join("|", QueueInfo));
        }
    }
}
