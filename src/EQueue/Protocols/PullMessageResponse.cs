using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class PullMessageResponse
    {
        public long? NextOffset { get; private set; }
        public IEnumerable<QueueMessage> Messages { get; private set; }

        public PullMessageResponse(IEnumerable<QueueMessage> messages)
        {
            Messages = messages;
        }
        public PullMessageResponse(IEnumerable<QueueMessage> messages, long nextOffset) : this(messages)
        {
            NextOffset = nextOffset;
        }
    }
}
