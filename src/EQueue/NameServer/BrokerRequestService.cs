using System;
using System.Linq;
using ECommon.Extensions;
using ECommon.Remoting;
using EQueue.Protocols.NameServers.Requests;

namespace EQueue.NameServer
{
    public class BrokerRequestService
    {
        private NameServerController _nameServerController;

        public BrokerRequestService(NameServerController nameServerController)
        {
            _nameServerController = nameServerController;
        }

        public void ExecuteActionToAllClusterBrokers(string clusterName, Action<SocketRemotingClient> action)
        {
            var request = new GetClusterBrokersRequest
            {
                ClusterName = clusterName,
                OnlyFindMaster = true
            };
            var endpointList = _nameServerController.ClusterManager.GetClusterBrokers(request).Select(x => x.AdminAddress.ToEndPoint());
            var remotingClientList = endpointList.ToRemotingClientList(_nameServerController.Setting.SocketSetting);

            foreach (var remotingClient in remotingClientList)
            {
                remotingClient.Start();
            }

            try
            {
                foreach (var remotingClient in remotingClientList)
                {
                    action(remotingClient);
                }
            }
            finally
            {
                foreach (var remotingClient in remotingClientList)
                {
                    remotingClient.Shutdown();
                }
            }
        }
    }
}
