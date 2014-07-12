using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryTopicConsumeInfoRequest
    {
        public string GroupName { get; private set; }
        public string Topic { get; private set; }

        public QueryTopicConsumeInfoRequest(string groupName, string topic)
        {
            GroupName = groupName;
            Topic = topic;
        }
    }
}
