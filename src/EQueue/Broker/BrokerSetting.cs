using EQueue.Remoting;

namespace EQueue.Broker
{
    public class BrokerSetting
    {
        private static BrokerSetting _default = new BrokerSetting
        {
            SendMessageSocketSetting = new SocketSetting { Address = "127.0.0.1", Port = 5000, Backlog = 5000 },
            PullMessageSocketSetting = new SocketSetting { Address = "127.0.0.1", Port = 5001, Backlog = 5000 },
            HeartbeatSocketSetting = new SocketSetting { Address = "127.0.0.1", Port = 5002, Backlog = 5000 }
        };

        public SocketSetting SendMessageSocketSetting { get; set; }
        public SocketSetting PullMessageSocketSetting { get; set; }
        public SocketSetting HeartbeatSocketSetting { get; set; }

        public static BrokerSetting Default
        {
            get { return _default; }
        }
    }
}
