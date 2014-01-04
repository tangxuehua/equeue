using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using EQueue.Infrastructure.IoC;
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
        private bool _running;

        public ServerSocket()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socketService = new SocketService();
            _newClientSocketSignal = new ManualResetEvent(false);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _running = false;
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
        public void Start(Action<ReceiveContext> messageReceivedCallback)
        {
            _messageReceivedCallback = messageReceivedCallback;
            _running = true;
            _logger.InfoFormat("Server is listening address:{0}", _socket.LocalEndPoint.ToString());

            while (_running)
            {
                _newClientSocketSignal.Reset();

                try
                {
                    _socket.BeginAccept((asyncResult) =>
                    {
                        var clientSocket = _socket.EndAccept(asyncResult);
                        _logger.InfoFormat("----Accepted new client.");
                        _newClientSocketSignal.Set();
                        _socketService.ReceiveMessage(clientSocket, (receivedMessage) =>
                        {
                            var receiveContext = new ReceiveContext(clientSocket, receivedMessage, context =>
                            {
                                _socketService.SendMessage(context.ReplySocket, context.ReplyMessage, sendResult => { });
                            });
                            _messageReceivedCallback(receiveContext);
                        });
                    }, _socket);
                }
                catch (SocketException socketException)
                {
                    _logger.Error(string.Format("Socket exception, ErrorCode:{0}", socketException.SocketErrorCode), socketException);
                }
                catch (Exception ex)
                {
                    _logger.Error("Unknown socket exception.", ex);
                }

                _newClientSocketSignal.WaitOne();
            }
        }
        public void Shutdown()
        {
            _socket.Shutdown(SocketShutdown.Both);
            _socket.Close();
            _running = false;
        }
    }
}
