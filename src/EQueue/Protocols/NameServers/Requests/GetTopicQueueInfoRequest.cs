using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class GetTopicQueueInfoRequest
    {
        public string ClusterName { get; set; }
        public string Topic { get; set; }
        public bool OnlyFindMaster { get; set; }
    }
}
