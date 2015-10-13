using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class RemoveQueueRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public RemoveQueueRequest(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
        }
    }
}
