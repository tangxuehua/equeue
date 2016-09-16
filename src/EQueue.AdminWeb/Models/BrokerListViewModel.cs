using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class BrokerListViewModel
    {
        public IEnumerable<BrokerInfo> BrokerList { get; set; }
    }
}