using System.Net.Sockets;

namespace EQueue.Remoting
{
    public class SocketChannel : IChannel
    {
        public Socket Socket { get; private set; }

        public SocketChannel(Socket socket)
        {
            Socket = socket;
        }

        public string RemotingAddress
        {
            get { return Socket.RemoteEndPoint.ToString(); }
        }

        public void Close()
        {
            Socket.Close();
        }

        public override string ToString()
        {
            return RemotingAddress;
        }
    }
}
