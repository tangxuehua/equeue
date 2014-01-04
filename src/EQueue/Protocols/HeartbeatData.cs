using System;
using System.Collections.Generic;

namespace EQueue.Protocols
{
    [Serializable]
    public class HeartbeatData
    {
        public string ClientId { get; private set; }
        public ConsumerData ConsumerData { get; private set; }

        public HeartbeatData(string clientId, ConsumerData consumerData)
        {
            ClientId = clientId;
            ConsumerData = consumerData;
        }

        public override string ToString()
        {
            return string.Format("[ClientId:{0}, ConsumerData:{1}]", ClientId, ConsumerData);
        }
    }
}
