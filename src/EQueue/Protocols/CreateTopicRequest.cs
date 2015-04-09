using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class CreateTopicRequest
    {
        public string Topic { get; private set; }
        public int InitialQueueCount { get; private set; }

        public CreateTopicRequest(string topic, int initialQueueCount)
        {
            Topic = topic;
            InitialQueueCount = initialQueueCount;
        }
    }
}
