using System.Collections.Generic;
using System.Linq;

namespace EQueue.Protocols
{
    public class TopicRouteInfo
    {
        public string Topic { get; set; }
        public int QueueId { get; set; }
        public BrokerInfo BrokerInfo { get; set; }

        public override string ToString()
        {
            return string.Format("[Topic: {0}, QueueId: {1}, BrokerInfo: {2}]", Topic, QueueId, BrokerInfo);
        }
    }
    public class BrokerInfo
    {
        public string BrokerName { get; set; }
        public IDictionary<string /*BrokerAddress*/, int /*BrokerRole*/> BrokerAddresses { get; set; }

        public BrokerInfo()
        {
            BrokerAddresses = new Dictionary<string, int>();
        }

        public override string ToString()
        {
            return string.Format("[BrokerName: {0}, BrokerAddresses: {1}]", BrokerName, string.Join("|", BrokerAddresses.Select(x => string.Format("[Address:{0}, Role:{1}]", x.Key, x.Value))));
        }
    }
}
