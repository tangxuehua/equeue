using EQueue.Infrastructure;
using EQueue.Remoting;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        private static BrokerSetting _default = new BrokerSetting
        {
            ProducerSocketSetting = new SocketSetting { Address = Utils.GetLocalIPV4(), Port = 5000, Backlog = 5000 },
            ConsumerSocketSetting = new SocketSetting { Address = Utils.GetLocalIPV4(), Port = 5001, Backlog = 5000 }
        };

        public SocketSetting ProducerSocketSetting { get; set; }
        public SocketSetting ConsumerSocketSetting { get; set; }

        public static BrokerSetting Default
        {
            get { return _default; }
        }
    }
}
