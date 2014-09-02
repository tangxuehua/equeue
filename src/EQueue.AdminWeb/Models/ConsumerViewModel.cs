using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class ConsumerViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<ConsumerInfo> ConsumerInfos { get; set; }
    }
}