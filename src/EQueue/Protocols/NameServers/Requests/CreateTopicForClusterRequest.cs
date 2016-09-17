using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class CreateTopicForClusterRequest
    {
        public string ClusterName { get; set; }
        public string Topic { get; set; }
        public int? InitialQueueCount { get; set; }
    }
}
