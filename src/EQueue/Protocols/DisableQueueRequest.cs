using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class DisableQueueRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public DisableQueueRequest(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
        }
    }
}
