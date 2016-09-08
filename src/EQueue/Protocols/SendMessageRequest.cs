using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class SendMessageRequest
    {
        public int QueueId { get; set; }
        public Message Message { get; set; }
    }
}
