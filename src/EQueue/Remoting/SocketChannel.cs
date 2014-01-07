using System.Net.Sockets;

namespace EQueue.Remoting
{
    public class SocketChannel : IChannel
    {
        private readonly Socket _socket;

        public SocketChannel(Socket socket)
        {
            _socket = socket;
        }

        public string RemotingAddress
        {
            get { return _socket.RemoteEndPoint.ToString(); }
        }

        public void Close()
        {
            _socket.Close();
        }

        public override string ToString()
        {
            return RemotingAddress;
        }
    }
}
