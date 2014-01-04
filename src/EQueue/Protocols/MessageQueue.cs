using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class MessageQueue
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }

        public MessageQueue(string topic, int queueId)
        {
            Topic = topic;
            QueueId = queueId;
        }

        public override string ToString()
        {
            return string.Format("[Topic={0}, QueueId={1}]", Topic, QueueId);
        }
    }
}
