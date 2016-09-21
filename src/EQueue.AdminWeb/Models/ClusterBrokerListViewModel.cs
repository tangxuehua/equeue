using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class ClusterBrokerListViewModel
    {
        public string ClusterName { get; set; }
        public IEnumerable<BrokerStatusInfo> BrokerList { get; set; }
    }
}