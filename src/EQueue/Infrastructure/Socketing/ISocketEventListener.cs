using System.Net.Sockets;

namespace EQueue.Infrastructure.Socketing
{
    public interface ISocketEventListener
    {
        void OnNewSocketAccepted(SocketInfo socketInfo);
        void OnSocketDisconnected(SocketInfo socketInfo);
    }
}
