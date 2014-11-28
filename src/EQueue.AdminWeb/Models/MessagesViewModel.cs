using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class MessagesViewModel
    {
        public string Topic { get; set; }
        public int? QueueId { get; set; }
        public int? Code { get; set; }
        public IEnumerable<QueueMessage> Messages { get; set; }
    }
}