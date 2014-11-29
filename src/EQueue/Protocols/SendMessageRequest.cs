using System;
using EQueue.Protocols;

namespace EQueue.Protocols
{
    [Serializable]
    public class SendMessageRequest
    {
        public string RoutingKey { get; set; }
        public int QueueId { get; set; }
        public Message Message { get; set; }
    }
}
