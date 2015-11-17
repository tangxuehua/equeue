using System;
using ECommon.Socketing;

namespace EQueue.Broker.Client
{
    public class ConsumerHeartbeatInfo
    {
        public string ConsumerId { get; private set; }
        public ITcpConnection Connection { get; private set; }
        public DateTime LastHeartbeatTime { get; set; }

        public ConsumerHeartbeatInfo(string consumerId, ITcpConnection connection)
        {
            ConsumerId = consumerId;
            Connection = connection;
        }

        public bool IsTimeout(double timeoutMilliseconds)
        {
            return (DateTime.Now - LastHeartbeatTime).TotalMilliseconds >= timeoutMilliseconds;
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId: {0}, LastHeartbeatTime: {1}]", ConsumerId, LastHeartbeatTime);
        }
    }
}
