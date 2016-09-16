using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class GetProducerListRequest
    {
        public string ClusterName { get; set; }
        public bool OnlyFindMaster { get; set; }
    }
}
