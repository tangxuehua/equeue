using System;

namespace EQueue.Protocols.Brokers.Requests
{
    [Serializable]
    public class AddQueueRequest
    {
        public string Topic { get; private set; }

        public AddQueueRequest(string topic)
        {
            Topic = topic;
        }
    }
}
