using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryConsumerRequest
    {
        public string GroupName { get; private set; }
        public string Topic { get; private set; }

        public QueryConsumerRequest(string groupName, string topic)
        {
            GroupName = groupName;
            Topic = topic;
        }

        public override string ToString()
        {
            return string.Format("[GroupName:{0}, Topic:{1}]", GroupName, Topic);
        }
    }
}
