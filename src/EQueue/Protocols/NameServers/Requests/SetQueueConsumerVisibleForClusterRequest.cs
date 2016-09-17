using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class SetQueueConsumerVisibleForClusterRequest
    {
        public string ClusterName { get; set; }
        public string Topic { get; set; }
        public int QueueId { get; set; }
        public bool Visible { get; set; }
    }
}
