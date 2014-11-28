using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class MessageViewModel
    {
        public long MessageOffset { get; set; }
        public QueueMessage Message { get; set; }
        public string MessageContent { get; set; }
    }
}