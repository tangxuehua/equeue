using System;
using EQueue.Remoting;

namespace EQueue.Broker.Client
{
    public class ClientChannelInfo
    {
        public Channel Channel { get; private set; }
        public string ClientId { get; private set; }
        public int Version { get; private set; }
        public DateTime LastUpdateTime { get; set; }

        public ClientChannelInfo(Channel channel, string clientId, int version)
        {
            Channel = channel;
            ClientId = clientId;
            Version = version;
            LastUpdateTime = DateTime.Now;
        }
    }
}
