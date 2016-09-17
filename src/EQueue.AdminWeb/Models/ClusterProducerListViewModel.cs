using System.Collections.Generic;
using EQueue.Protocols.NameServers;

namespace EQueue.AdminWeb.Models
{
    public class ClusterProducerListViewModel
    {
        public string ClusterName { get; set; }
        public IEnumerable<BrokerProducerListInfo> ProducerList { get; set; }
    }
}