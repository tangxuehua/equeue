using System.Collections.Generic;
using EQueue.Protocols.Brokers;

namespace EQueue.AdminWeb.Models
{
    public class ClusterConsumerListViewModel
    {
        public string ClusterName { get; set; }
        public string Group { get; set; }
        public string Topic { get; set; }
        public IEnumerable<ConsumerViewModel> ConsumerList { get; set; }
    }
    public class ConsumerViewModel
    {
        public ConsumerInfo ConsumerInfo { get; set; }
        public BrokerInfo BrokerInfo { get; set; }
    }
}