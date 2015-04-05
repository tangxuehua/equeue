using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class MessagesViewModel
    {
        public string MessageId { get; set; }
        public string Topic { get; set; }
        public int? QueueId { get; set; }
        public int? Code { get; set; }
        public string RoutingKey { get; set; }
        public long Total { get; set; }
        public IEnumerable<QueueMessage> Messages { get; set; }
    }
}