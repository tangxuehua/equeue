using System.Collections.Generic;
using EQueue.Protocols.NameServers;

namespace EQueue.AdminWeb.Models
{
    public class ClusterTopicQueueListViewModel
    {
        public string Topic { get; set; }
        public IEnumerable<BrokerTopicQueueInfo> TopicQueueInfoList { get; set; }
    }
}