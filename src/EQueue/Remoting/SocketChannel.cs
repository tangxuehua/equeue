using System.Net.Sockets;
using EQueue.Infrastructure.Socketing;

namespace EQueue.Remoting
{
    public class SocketChannel : IChannel
    {
        public SocketInfo SocketInfo { get; private set; }

        public SocketChannel(SocketInfo socketInfo)
        {
            SocketInfo = socketInfo;
        }

        public string RemotingAddress
        {
            get { return SocketInfo.SocketRemotingEndpointAddress; }
        }

        public void Close()
        {
            SocketInfo.InnerSocket.Close();
        }

        public override string ToString()
        {
            return RemotingAddress;
        }
    }
}
