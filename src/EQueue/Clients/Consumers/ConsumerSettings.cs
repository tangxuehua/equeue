using ECommon.Socketing;

namespace EQueue.Clients.Consumers
{
    public class ConsumerSetting
    {
        public string BrokerAddress { get; set; }
        public int BrokerPort { get; set; }
        public int RebalanceInterval { get; set; }
        public int UpdateTopicQueueCountInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int PersistConsumerOffsetInterval { get; set; }
        public PullRequestSetting PullRequestSetting { get; set; }
        public MessageHandleMode MessageHandleMode { get; set; }

        public ConsumerSetting()
        {
            BrokerAddress = SocketUtils.GetLocalIPV4().ToString();
            BrokerPort = 5001;
            RebalanceInterval = 1000 * 5;
            HeartbeatBrokerInterval = 1000 * 5;
            UpdateTopicQueueCountInterval = 1000 * 5;
            PersistConsumerOffsetInterval = 1000 * 5;
            PullRequestSetting = new PullRequestSetting();
            MessageHandleMode = MessageHandleMode.Parallel;
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
