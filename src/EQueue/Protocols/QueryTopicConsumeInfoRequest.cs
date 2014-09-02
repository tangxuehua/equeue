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

        public override string ToString()
        {
            return string.Format("[GroupName:{0}, Topic:{1}]", GroupName, Topic);
        }
    }
}
