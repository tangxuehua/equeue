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
            Channel = channel;
            ClientId = clientId;
        }
    }
}
