using ECommon.Socketing;

namespace EQueue.Clients.Producers
{
    public class ProducerSetting
    {
        private static ProducerSetting _default = new ProducerSetting();

        public string BrokerAddress { get; set; }
        public int BrokerPort { get; set; }
        public int SendMessageTimeoutMilliseconds { get; set; }

        public static ProducerSetting Default { get { return _default; } }

        public ProducerSetting()
        {
            BrokerAddress = SocketUtils.GetLocalIPV4().ToString();
            BrokerPort = 5000;
            SendMessageTimeoutMilliseconds = 1000 * 10;
        }
        public ProducerSetting(string brokerAddress) : this()
        {
            BrokerAddress = brokerAddress;
        }

        public override string ToString()
        {
            return string.Format("[BrokerAddress={0}, BrokerPort={1}, SendMessageTimeoutMilliseconds={2}]",
                BrokerAddress,
                BrokerPort,
                SendMessageTimeoutMilliseconds);
        }
    }
}
