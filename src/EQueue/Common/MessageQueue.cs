using System;

namespace EQueue.Common
{
    [Serializable]
    public class MessageQueue
    {
        public string Topic { get; set; }
        public string BrokerAddress { get; set; }
        public int QueueId { get; set; }

        public MessageQueue(string topic, string brokerAddress, int queueId)
        {
            Topic = topic;
            BrokerAddress = brokerAddress;
            QueueId = queueId;
        }

        public override string ToString()
        {
            return string.Format("MessageQueue [Topic={0}, BrokerAddress={1}, QueueId={2}]", Topic, BrokerAddress, QueueId);
        }
    }
}
