using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class BrokerTopicQueueListViewModel
    {
        public string Topic { get; set; }
        public IEnumerable<TopicQueueInfo> TopicQueueInfoList { get; set; }
    }
}