using System;

namespace EQueue.Protocols
{
    [Serializable]
    public class QueryMessageRequest
    {
        public string Topic { get; private set; }
        public int? QueueId { get; private set; }
        public int? Code { get; private set; }

        public QueryMessageRequest(string topic, int? queueId, int? code)
        {
            Topic = topic;
            QueueId = queueId;
            Code = code;
        }

        public override string ToString()
        {
            return string.Format("[Topic:{0}, QueueId:{1}, Code:{2}]", Topic, QueueId, Code);
        }
    }
}
