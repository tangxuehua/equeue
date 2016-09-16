using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class TopicConsumeInfoViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<TopicConsumeInfo> TopicConsumeInfoList { get; set; }
    }
}