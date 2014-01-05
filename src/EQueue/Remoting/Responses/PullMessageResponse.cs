using System.Collections.Generic;
using EQueue.Protocols;

namespace EQueue.Remoting.Responses
{
    public class PullMessageResponse
    {
        public IEnumerable<QueueMessage> Messages { get; private set; }

        public PullMessageResponse(IEnumerable<QueueMessage> messages)
        {
            Messages = messages;
        }
    }
}
