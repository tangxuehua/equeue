using System.Collections.Generic;
using EQueue.Protocols.NameServers;

namespace EQueue.AdminWeb.Models
{
    public class ClusterConsumerListViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<BrokerConsumerListInfo> ConsumerList { get; set; }
    }
}