using ECommon.Socketing;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        public string BrokerAddress { get; set; }
        public int BrokerPort { get; set; }
        public int SendMessageTimeoutMilliseconds { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }

        public ProducerSetting()
        {
            BrokerAddress = SocketUtils.GetLocalIPV4().ToString();
            BrokerPort = 5000;
            SendMessageTimeoutMilliseconds = 1000 * 10;
            UpdateTopicQueueCountInterval = 1000 * 5;
        }

        public override string ToString()
        {
            return string.Format("[BrokerAddress={0}, BrokerPort={1}, SendMessageTimeoutMilliseconds={2}, UpdateTopicQueueCountInterval={3}]",
                BrokerAddress,
                BrokerPort,
                SendMessageTimeoutMilliseconds,
                UpdateTopicQueueCountInterval);
        }
    }
}
