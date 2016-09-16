using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class GetTopicRouteInfoRequest
    {
        public ClientRole ClientRole { get; set; }
        public string ClusterName { get; set; }
        public string Topic { get; set; }
        public bool OnlyFindMaster { get; set; }
    }
}
