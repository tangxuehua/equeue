using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryConsumerInfoRequest
    {
        public string GroupName { get; private set; }
        public string Topic { get; private set; }

        public QueryConsumerInfoRequest(string groupName, string topic)
        {
            GroupName = groupName;
            Topic = topic;
        }
    }
}
