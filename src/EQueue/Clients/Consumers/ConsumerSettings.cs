namespace EQueue.Clients.Consumers
{
    public class ConsumerSettings
    {
        public string BrokerAddress { get; set; }
        public int UpdateTopicRouteDataInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int PersistConsumerOffsetInterval { get; set; }

        public ConsumerSettings()
        {
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
            return string.Format("ConsumerSettings [BrokerAddress={0}, HeartbeatBrokerInterval={1}, UpdateTopicRouteDataInterval={2}, PersistConsumerOffsetInterval={3}]",
                BrokerAddress,
                HeartbeatBrokerInterval,
                UpdateTopicRouteDataInterval,
                PersistConsumerOffsetInterval);
        }
    }
}
