using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryMessageRequest
    {
        public string Topic { get; private set; }
        public int? QueueId { get; private set; }
        public int? Code { get; private set; }
        public string RoutingKey { get; private set; }
        public int PageIndex { get; private set; }
        public int PageSize { get; private set; }

        public QueryMessageRequest(string topic, int? queueId, int? code, string routingKey, int pageIndex, int pageSize)
        {
            Topic = topic;
            QueueId = queueId;
            Code = code;
            RoutingKey = routingKey;
            PageIndex = pageIndex;
            PageSize = pageSize;
        }

        public override string ToString()
        {
            return string.Format("[Topic:{0}, QueueId:{1}, Code:{2}, RouringKey:{3}, PageIndex:{4}, PageSize:{5}]", Topic, QueueId, Code, RoutingKey, PageIndex, PageSize);
        }
    }
}
