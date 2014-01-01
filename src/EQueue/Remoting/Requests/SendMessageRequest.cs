using EQueue.Protocols;

namespace EQueue.Remoting.Requests
{
    public class SendMessageRequest
    {
        public Message Message { get; set; }
        public string Arg { get; set; }
    }
}
