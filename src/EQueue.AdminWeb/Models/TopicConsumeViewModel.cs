using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class TopicConsumeViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<TopicConsumeInfo> TopicConsumeInfos { get; set; }
    }
}