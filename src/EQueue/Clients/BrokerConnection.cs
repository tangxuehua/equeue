using ECommon.Remoting;
using EQueue.Protocols.Brokers;

namespace EQueue.Clients
{
    public class BrokerConnection
    {
        public BrokerInfo BrokerInfo { get; }
        public SocketRemotingClient RemotingClient { get; }
        public SocketRemotingClient AdminRemotingClient { get; }

        public BrokerConnection(BrokerInfo brokerInfo, SocketRemotingClient remotingClient, SocketRemotingClient adminRemotingClient)
        {
            BrokerInfo = brokerInfo;
            RemotingClient = remotingClient;
            AdminRemotingClient = adminRemotingClient;
        }

        public void Start()
        {
            RemotingClient.Start();
            AdminRemotingClient.Start();
        }
        public void Stop()
        {
            RemotingClient.Shutdown();
            AdminRemotingClient.Shutdown();
        }
    }
}
