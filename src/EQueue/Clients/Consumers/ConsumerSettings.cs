namespace EQueue.Clients.Consumers
{
    public class ConsumerSettings
    {
        private static ConsumerSettings _default = new ConsumerSettings();

        public string BrokerAddress { get; set; }
        public int BrokerPort { get; set; }
        public int UpdateTopicRouteDataInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int PersistConsumerOffsetInterval { get; set; }

        public static ConsumerSettings Default { get { return _default; } }

        public ConsumerSettings()
        {
            BrokerAddress = "127.0.0.1";
            BrokerPort = 5001;
            HeartbeatBrokerInterval = 1000 * 30;
            UpdateTopicRouteDataInterval = 1000 * 30;
            PersistConsumerOffsetInterval = 1000 * 5;
        }
        public ConsumerSettings(string brokerAddress) : this()
        {
            BrokerAddress = brokerAddress;
        }

        public override string ToString()
        {
            return string.Format("ConsumerSettings [BrokerAddress={0}, BrokerPort={1}, HeartbeatBrokerInterval={2}, UpdateTopicRouteDataInterval={3}, PersistConsumerOffsetInterval={4}]",
                BrokerAddress,
                BrokerPort,
                HeartbeatBrokerInterval,
                UpdateTopicRouteDataInterval,
                PersistConsumerOffsetInterval);
        }
    }
}
