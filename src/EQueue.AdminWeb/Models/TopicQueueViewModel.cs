using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class TopicQueueViewModel
    {
        public string Topic { get; set; }
        public IEnumerable<TopicQueueInfo> TopicQueueInfos { get; set; }
    }
}