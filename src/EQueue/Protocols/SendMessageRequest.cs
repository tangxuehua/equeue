using EQueue.Protocols;

namespace EQueue.Protocols
{
    public class SendMessageRequest
    {
        public Message Message { get; set; }
        public string Arg { get; set; }
    }
}
