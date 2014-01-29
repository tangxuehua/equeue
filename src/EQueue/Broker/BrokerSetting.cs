using ECommon.Remoting;
using ECommon.Socketing;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        private static BrokerSetting _default = new BrokerSetting
        {
            ProducerSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5000, Backlog = 5000 },
            ConsumerSocketSetting = new SocketSetting { Address = SocketUtils.GetLocalIPV4().ToString(), Port = 5001, Backlog = 5000 },
            NotifyWhenMessageArrived = true
        };

        public SocketSetting ProducerSocketSetting { get; set; }
        public SocketSetting ConsumerSocketSetting { get; set; }
        public bool NotifyWhenMessageArrived { get; set; }

        public static BrokerSetting Default
        {
            get { return _default; }
        }
    }
}
