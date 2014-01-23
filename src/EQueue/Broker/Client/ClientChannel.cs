using System;
using ECommon.Remoting;

namespace EQueue.Broker.Client
{
    public class ClientChannel
    {
        public string ClientId { get; private set; }
        public IChannel Channel { get; private set; }
        public DateTime LastUpdateTime { get; set; }

        public ClientChannel(string clientId, IChannel channel)
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
