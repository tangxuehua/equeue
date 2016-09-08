using System.Collections.Generic;
using System.Net;
using ECommon.Socketing;
using EQueue.Protocols;

namespace EQueue.Clients
{
    public class ClientSetting
    {
        public ClientRole ClientRole { get; set; }
        public string ClientName { get; set; }
        public string ClusterName { get; set; }
        public IEnumerable<IPEndPoint> NameServerList { get; set; }
        public SocketSetting SocketSetting { get; set; }
        public bool OnlyFindMasterBroker { get; set; }
        public int SendHeartbeatInterval { get; set; }
        public int RefreshBrokerAndTopicRouteInfoInterval { get; set; }
    }
}
