using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class EnableQueueRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public EnableQueueRequest(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
        }
    }
}
