using EQueue.Protocols;

namespace EQueue.Protocols
{
    public class SendMessageRequest
    {
        public int QueueId { get; set; }
        public Message Message { get; set; }
    }
}
