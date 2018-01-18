using System;

namespace EQueue.Protocols.Brokers.Requests
{
    [Serializable]
    public class GetMessageDetailByQueueOffsetRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public long QueueOffset { get; private set; }

        public GetMessageDetailByQueueOffsetRequest(string topic, int queueId, long queueOffset)
        {
            Topic = topic;
            QueueId = queueId;
            QueueOffset = queueOffset;
        }

        public override string ToString()
        {
            return string.Format("[Topic:{0},QueueId:{1},QueueOffset:{2}]", Topic, QueueId, QueueOffset);
        }
    }
}
