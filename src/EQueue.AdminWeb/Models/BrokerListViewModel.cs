using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class BrokerListViewModel
    {
        public IEnumerable<BrokerInfo> BrokerList { get; set; }
    }
}