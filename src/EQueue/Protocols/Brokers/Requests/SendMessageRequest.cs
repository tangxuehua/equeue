using System;

namespace EQueue.Protocols.Brokers.Requests
{
    [Serializable]
    public class SendMessageRequest
    {
        public int QueueId { get; set; }
        public Message Message { get; set; }
    }
}
