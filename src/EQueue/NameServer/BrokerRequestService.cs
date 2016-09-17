using System;
using System.Linq;
using ECommon.Remoting;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

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
            var remotingClientList = RemotingClientUtils.CreateRemotingClientList(endpointList, _nameServerController.Setting.SocketSetting);

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
