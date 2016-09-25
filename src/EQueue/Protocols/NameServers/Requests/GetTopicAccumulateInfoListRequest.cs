using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class GetTopicAccumulateInfoListRequest
    {
        public long AccumulateThreshold { get; set; }
    }
}
