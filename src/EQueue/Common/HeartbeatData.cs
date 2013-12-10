using System;
using System.Collections.Generic;

namespace EQueue.Common
{
    [Serializable]
    public class HeartbeatData
    {
        public string ClientId { get; private set; }
        public IEnumerable<ProducerData> ProducerDatas { get; private set; }
        public IEnumerable<ConsumerData> ConsumerDatas { get; private set; }

        public HeartbeatData(string clientId, IEnumerable<ProducerData> producerDatas, IEnumerable<ConsumerData> consumerDatas)
        {
            ClientId = clientId;
            ProducerDatas = producerDatas;
            ConsumerDatas = consumerDatas;
        }

        public override string ToString()
        {
            return string.Format("[ClientId:{0}, ProducerData:{1}, ConsumerData:{2}]", ClientId, string.Join(",", ProducerDatas), string.Join(",", ConsumerDatas));
        }
    }
}
