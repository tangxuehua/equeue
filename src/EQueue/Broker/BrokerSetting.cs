using ECommon.Remoting;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        private static BrokerSetting _default = new BrokerSetting
        {
            ProducerSocketSetting = new SocketSetting { Address = "127.0.0.1", Port = 5000, Backlog = 5000 },
            ConsumerSocketSetting = new SocketSetting { Address = "127.0.0.1", Port = 5001, Backlog = 5000 }
        };

        public SocketSetting ProducerSocketSetting { get; set; }
        public SocketSetting ConsumerSocketSetting { get; set; }

        public static BrokerSetting Default
        {
            get { return _default; }
        }
    }
}
