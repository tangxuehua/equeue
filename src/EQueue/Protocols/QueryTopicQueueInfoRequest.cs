using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryTopicQueueInfoRequest
    {
        public string Topic { get; private set; }

        public QueryTopicQueueInfoRequest(string topic)
        {
            Topic = topic;
        }
    }
}
