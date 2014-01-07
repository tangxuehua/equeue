using System;
using System.Net.Sockets;

namespace EQueue.Infrastructure.Socketing
{
    public class SocketInfo
    {
        public string SocketRemotingEndpointAddress { get; private set; }
        public Socket InnerSocket { get; private set; }

        public SocketInfo(Socket socket)
        {
            if (!socket.Connected)
            {
                throw new Exception("Invalid socket.");
            }
            InnerSocket = socket;
            SocketRemotingEndpointAddress = socket.RemoteEndPoint.ToString();
        }
    }
}
