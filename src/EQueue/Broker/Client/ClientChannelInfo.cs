using System;
using EQueue.Remoting;

namespace EQueue.Broker.Client
{
    public class ClientChannelInfo
    {
        public string ClientId { get; private set; }
        public IChannel Channel { get; private set; }
        public DateTime LastUpdateTime { get; set; }

        public ClientChannelInfo(string clientId, IChannel channel)
        {
            ClientId = clientId;
            Channel = channel;
        }

        public override string ToString()
        {
            return string.Format("[ClientId:{0}, Channel:{1}, LastUpdateTime:{2}]", ClientId, Channel, LastUpdateTime);
        }
    }
}
