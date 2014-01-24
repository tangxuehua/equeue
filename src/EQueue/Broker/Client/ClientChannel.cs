using System;
using ECommon.Remoting;

namespace EQueue.Broker.Client
{
    public class ClientChannel
    {
        public string ClientId { get; private set; }
        public IChannel Channel { get; private set; }
        public DateTime LastUpdateTime { get; set; }
        public DateTime? ClosedTime { get; private set; }

        public ClientChannel(string clientId, IChannel channel)
        {
            ClientId = clientId;
            Channel = channel;
        }

        public bool IsTimeout(double timeoutMilliseconds)
        {
            return DateTime.Now > LastUpdateTime.AddMilliseconds(timeoutMilliseconds);
        }
        public void Close()
        {
            Channel.Close();
            ClosedTime = DateTime.Now;
        }

        public override string ToString()
        {
            return string.Format("[ClientId:{0}, Channel:{1}, LastUpdateTime:{2}, ClosedTime:{3}]", ClientId, Channel, LastUpdateTime, ClosedTime);
        }
    }
}
