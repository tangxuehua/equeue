using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class DeleteConsumerGroupForClusterRequest
    {
        public string ClusterName { get; set; }
        public string GroupName { get; set; }
    }
}
