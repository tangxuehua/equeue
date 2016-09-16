using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class TopicQueueViewModel
    {
        public string Topic { get; set; }
        public IEnumerable<TopicQueueInfo> TopicQueueInfos { get; set; }
    }
}