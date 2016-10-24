using System;
using System.Collections.Generic;

namespace EQueue.Protocols.Brokers.Requests
{
    [Serializable]
    public class BatchSendMessageRequest
    {
        public int QueueId { get; set; }
        public IEnumerable<Message> Messages { get; set; }
        public string ProducerAddress { get; set; }
    }
}
