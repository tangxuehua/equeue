using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class BrokerTopicConsumeListViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<TopicConsumeInfo> TopicConsumeInfoList { get; set; }
    }
}