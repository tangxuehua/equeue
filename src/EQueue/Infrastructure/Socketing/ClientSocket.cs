using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace EQueue.Infrastructure.Socketing
{
    public class ClientSocket
    {
        private Socket _innerSocket;
        private SocketService _socketService;

        public ClientSocket()
        {
            _innerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socketService = new SocketService();
        }

        public ClientSocket Connect(string address, int port)
        {
            _innerSocket.Connect(new IPEndPoint(IPAddress.Parse(address), port));
            return this;
        }
        public ClientSocket Start(Action<byte[]> replyMessageReceivedCallback)
        {
            Task.Factory.StartNew(() =>
            {
                _socketService.ReceiveMessage(_innerSocket, (reply) =>
                {
                    try
                    {
                        replyMessageReceivedCallback(reply);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                });
                new ManualResetEvent(false).WaitOne();
            });
            return this;
        }
        public ClientSocket Shutdown()
        {
            _innerSocket.Shutdown(SocketShutdown.Both);
            _innerSocket.Close();
            return this;
        }
        public ClientSocket SendMessage(byte[] messageContent, Action<SendResult> messageSendCallback)
        {
            _socketService.SendMessage(_innerSocket, messageContent, messageSendCallback);
            return this;
        }
    }
}
