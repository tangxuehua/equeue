using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class GetClusterBrokersRequest
    {
        public string ClusterName { get; set; }
        public bool OnlyFindMaster { get; set; }
    }
}
