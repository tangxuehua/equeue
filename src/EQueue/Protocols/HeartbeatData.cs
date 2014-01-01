using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class HeartbeatData
    {
        public string ClientId { get; private set; }
        public IEnumerable<ConsumerData> ConsumerDatas { get; private set; }

        public HeartbeatData(string clientId, IEnumerable<ConsumerData> consumerDatas)
        {
            ClientId = clientId;
            ConsumerDatas = consumerDatas;
        }

        public override string ToString()
        {
            return string.Format("[ClientId:{0}, ConsumerData:{1}]", ClientId, string.Join(",", ConsumerDatas));
        }
    }
}
