using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryMessageResponse
    {
        public int Total { get; private set; }
        public IEnumerable<QueueMessage> Messages { get; private set; }

        public QueryMessageResponse(int total, IEnumerable<QueueMessage> messages)
        {
            Total = total;
            Messages = messages;
        }
    }
}
