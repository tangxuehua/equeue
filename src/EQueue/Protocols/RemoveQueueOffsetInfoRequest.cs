using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class RemoveQueueOffsetInfoRequest
    {
        public string ConsumerGroup { get; private set; }
        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public RemoveQueueOffsetInfoRequest(string consumerGroup, string topic, int queueId)
        {
            ConsumerGroup = consumerGroup;
            Topic = topic;
            QueueId = queueId;
        }
    }
}
