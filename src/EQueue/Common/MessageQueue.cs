using System;

namespace EQueue.Common
{
    [Serializable]
    public class MessageQueue
    {
        public string Topic { get; set; }
        public string BrokerName { get; set; }
        public int QueueId { get; set; }

        public MessageQueue(string topic, string brokerName, int queueId)
        {
            Topic = topic;
            BrokerName = brokerName;
            QueueId = queueId;
        }

        public override string ToString()
        {
            return string.Format("MessageQueue [Topic={0}, BrokerName={1}, QueueId={2}]", Topic, BrokerName, QueueId);
        }
    }
}
