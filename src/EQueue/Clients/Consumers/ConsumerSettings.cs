using EQueue.Infrastructure;

namespace EQueue.Clients.Consumers
{
    public class ConsumerSettings
    {
        private static ConsumerSettings _default = new ConsumerSettings();

        public string BrokerAddress { get; set; }
        public int BrokerPort { get; set; }
        public int RebalanceInterval { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int PersistConsumerOffsetInterval { get; set; }
        public PullRequestSetting PullRequestSetting { get; set; }
        public MessageHandleMode MessageHandleMode { get; set; }

        public static ConsumerSettings Default { get { return _default; } }

        public ConsumerSettings()
        {
            BrokerAddress = Utils.GetLocalIPV4();
            BrokerPort = 5001;
            RebalanceInterval = 1000 * 5;
            HeartbeatBrokerInterval = 1000 * 5;
            UpdateTopicQueueCountInterval = 1000 * 5;
            PersistConsumerOffsetInterval = 1000 * 5;
            PullRequestSetting = PullRequestSetting.Default;
            MessageHandleMode = MessageHandleMode.Parallel;
        }
        public ConsumerSettings(string brokerAddress) : this()
        {
            BrokerAddress = brokerAddress;
        }

        public override string ToString()
        {
            return string.Format("[BrokerAddress={0}, BrokerPort={1}, HeartbeatBrokerInterval={2}, UpdateTopicQueueCountInterval={3}, PersistConsumerOffsetInterval={4}, RebalanceInterval={5}, PullRequestSetting={6}, MessageHandleMode={7}]",
                BrokerAddress,
                BrokerPort,
                HeartbeatBrokerInterval,
                UpdateTopicQueueCountInterval,
                PersistConsumerOffsetInterval,
                RebalanceInterval,
                PullRequestSetting,
                MessageHandleMode);
        }
    }
}
