using ECommon.Remoting;
using EQueue.Protocols.Brokers;

namespace EQueue.Clients
{
    public class BrokerConnection
    {
        private readonly BrokerInfo _brokerInfo;
        private readonly SocketRemotingClient _remotingClient;
        private readonly SocketRemotingClient _adminRemotingClient;

        public BrokerInfo BrokerInfo
        {
            get { return _brokerInfo; }
        }
        public SocketRemotingClient RemotingClient
        {
            get { return _remotingClient; }
        }
        public SocketRemotingClient AdminRemotingClient
        {
            get { return _adminRemotingClient; }
        }

        public BrokerConnection(BrokerInfo brokerInfo, SocketRemotingClient remotingClient, SocketRemotingClient adminRemotingClient)
        {
            _brokerInfo = brokerInfo;
            _remotingClient = remotingClient;
            _adminRemotingClient = adminRemotingClient;
        }

        public void Start()
        {
            _remotingClient.Start();
            _adminRemotingClient.Start();
        }
        public void Stop()
        {
            _remotingClient.Shutdown();
            _adminRemotingClient.Shutdown();
        }
    }
}
