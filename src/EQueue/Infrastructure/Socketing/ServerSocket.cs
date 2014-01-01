using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using EQueue.Infrastructure.Logging;

namespace EQueue.Infrastructure.Socketing
{
    public class ServerSocket
    {
        private Socket _socket;
        private Action<ReceiveContext> _messageReceivedCallback;
        private ManualResetEvent _newClientSocketSignal;
        private SocketService _socketService;
        private ILogger _logger;

        public ServerSocket(SocketService socketService, ILoggerFactory loggerFactory)
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _newClientSocketSignal = new ManualResetEvent(false);
            _socketService = socketService;
            _logger = loggerFactory.Create(GetType().Name);
        }

        public ServerSocket Listen(int backlog)
        {
            _socket.Listen(backlog);
            return this;
        }
        public ServerSocket Bind(string address, int port)
        {
            _socket.Bind(new IPEndPoint(IPAddress.Parse(address), port));
            return this;
        }
        public ClientSocket Start(Action<ReceiveContext> messageReceivedCallback)
        {
            _messageReceivedCallback = messageReceivedCallback;

            _logger.DebugFormat("socket is listening address:{0}", _socket.LocalEndPoint.ToString());

            while (true)
            {
                _newClientSocketSignal.Reset();

                try
                {
                    _socket.BeginAccept((asyncResult) =>
                    {
                        var clientSocket = _socket.EndAccept(asyncResult);
                        _logger.DebugFormat("----accepted new client.");
                        _newClientSocketSignal.Set();
                        _socketService.ReceiveMessage(clientSocket, (receivedMessage) =>
                        {
                            var receiveContext = new ReceiveContext(clientSocket, receivedMessage, (context) =>
                            {
                                _socketService.SendMessage(context.ReplySocket, context.ReplyMessage, (reply) => { });
                            });
                            _messageReceivedCallback(receiveContext);
                        });
                    }, _socket);
                }
                catch (SocketException socketException)
                {
                    _logger.ErrorFormat("Socket exception, ErrorCode:{0}", socketException.SocketErrorCode);
                }
                catch (Exception ex)
                {
                    _logger.ErrorFormat("Unknown socket exception:{0}", ex);
                }

                _newClientSocketSignal.WaitOne();
            }
        }
    }
}
