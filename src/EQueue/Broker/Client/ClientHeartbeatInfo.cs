using System;
using ECommon.Socketing;

namespace EQueue.Broker.Client
{
    public class ClientHeartbeatInfo
    {
        public ITcpConnection Connection { get; private set; }
        public DateTime LastHeartbeatTime { get; set; }

        public ClientHeartbeatInfo(ITcpConnection connection)
        {
            Connection = connection;
        }

        public bool IsTimeout(double timeoutMilliseconds)
        {
            return (DateTime.Now - LastHeartbeatTime).TotalMilliseconds >= timeoutMilliseconds;
        }
    }
}
