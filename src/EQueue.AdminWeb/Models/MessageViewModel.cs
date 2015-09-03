using System;
using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.AdminWeb.Models
{
    public class MessageViewModel
    {
        public string SearchMessageId { get; set; }
        public string SearchMessageOffset { get; set; }
        public string MessageId { get; set; }
        public string MessageOffset { get; set; }
        public string QueueId { get; set; }
        public string QueueOffset { get; set; }
        public string Topic { get; set; }
        public string Code { get; set; }
        public string CreatedTime { get; set; }
        public string StoredTime { get; set; }
        public string RoutingKey { get; set; }
        public string Content { get; set; }
    }
}