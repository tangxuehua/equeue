using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class BrokerConsumerListViewModel
    {
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<ConsumerInfo> ConsumerList { get; set; }
    }
}