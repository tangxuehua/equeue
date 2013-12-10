using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace EQueue
{
    public class ClientConfig
    {
        public string InstanceName { get; set; }
        public string ClientIP { get; set; }
        public string BrokerAddress { get; set; }
        public string NameServerAddress { get; set; }
        public int PollNameServerInterval { get; set; }
        public int HeartbeatBrokerInterval { get; set; }
        public int PersistConsumerOffsetInterval { get; set; }

        public ClientConfig()
        {
            InstanceName = "DEFAULT";
            ClientIP = GetLocalIP();
            PollNameServerInterval = 1000 * 30;
            HeartbeatBrokerInterval = 1000 * 30;
            PersistConsumerOffsetInterval = 1000 * 5;
        }

        public string BuildClientId()
        {
            return string.Format("{0}@{1}", ClientIP, InstanceName);
        }

        private string GetLocalIP()
        {
            return Dns.GetHostEntry(Dns.GetHostName()).AddressList.Single(x => x.AddressFamily == AddressFamily.InterNetwork).ToString();
        }

        public override string ToString()
        {
            return string.Format("ClientConfig [NameServerAddress={0}, ClientIP={1}, InstanceName={2}, PollNameServerInterval={3}, HeartbeatBrokerInterval={4}, PersistConsumerOffsetInterval={5}]",
                NameServerAddress,
                ClientIP,
                InstanceName,
                PollNameServerInterval,
                HeartbeatBrokerInterval,
                PersistConsumerOffsetInterval);
        }
    }
}
