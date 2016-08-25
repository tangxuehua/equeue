using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class MessageQueue
    {
        public string Topic { get; private set; }
        public string BrokerName { get; private set; }
        public int QueueId { get; private set; }

        public MessageQueue(string topic, string brokerName, int queueId)
        {
            Topic = topic;
            BrokerName = brokerName;
            QueueId = queueId;
        }

        public override string ToString()
        {
            return string.Format("[Topic={0}, BrokerName={1}, QueueId={2}]", Topic, BrokerName, QueueId);
        }
    }
}
