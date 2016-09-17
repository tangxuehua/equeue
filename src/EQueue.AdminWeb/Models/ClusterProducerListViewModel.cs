using System.Collections.Generic;
using EQueue.Protocols.NameServers;

namespace EQueue.AdminWeb.Models
{
    public class ClusterProducerListViewModel
    {
        public IEnumerable<BrokerProducerListInfo> ProducerList { get; set; }
    }
}