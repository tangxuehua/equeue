using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using EQueue.Infrastructure.Extensions;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Scheduling;

namespace EQueue.Infrastructure.Socketing
{
    public class ServerSocket
    {
        private Socket _socket;
        private ConcurrentDictionary<string, SocketInfo> _clientSocketDict;
        private Action<ReceiveContext> _messageReceivedCallback;
        private ManualResetEvent _newClientSocketSignal;
        private SocketService _socketService;
        private ISocketEventListener _socketEventListener;
        private IScheduleService _scheduleService;
        private ILogger _logger;
        private bool _running;

        public ServerSocket(ISocketEventListener socketEventListener)
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _clientSocketDict = new ConcurrentDictionary<string, SocketInfo>();
            _socketEventListener = socketEventListener;
            _socketService = new SocketService(NotifySocketDisconnected);
            _newClientSocketSignal = new ManualResetEvent(false);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
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
            _scheduleService.ScheduleTask(CheckClientSocketConnection, 3000, 3000);
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
                        var clientSocketInfo = new SocketInfo(clientSocket);
                        _clientSocketDict.TryAdd(clientSocketInfo.SocketRemotingEndpointAddress, clientSocketInfo);
                        _logger.InfoFormat("----Accepted new client:{0}", clientSocketInfo.SocketRemotingEndpointAddress);
                        _newClientSocketSignal.Set();
                        _socketService.ReceiveMessage(clientSocketInfo, (receivedMessage) =>
                        {
                            var receiveContext = new ReceiveContext(clientSocketInfo, receivedMessage, context =>
                            {
                                _socketService.SendMessage(context.ReplySocketInfo.InnerSocket, context.ReplyMessage, sendResult => { });
                            });
                            _messageReceivedCallback(receiveContext);
                        });
                    }, _socket);
                }
                catch (SocketException socketException)
                {
                    _logger.Error(string.Format("Socket accept exception, ErrorCode:{0}", socketException.SocketErrorCode), socketException);
                }
                catch (Exception ex)
                {
                    _logger.Error("Unknown socket accept exception.", ex);
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

        private void CheckClientSocketConnection()
        {
            var disconnectedSockets = new List<SocketInfo>();
            foreach (var entry in _clientSocketDict)
            {
                if (!IsSocketConnected(entry.Value))
                {
                    disconnectedSockets.Add(entry.Value);
                }
            }
            foreach (var socket in disconnectedSockets)
            {
                NotifySocketDisconnected(socket);
            }
        }

        private bool IsSocketConnected(SocketInfo clientSocketInfo)
        {
            var clientSocket = clientSocketInfo.InnerSocket;
            var part1 = clientSocket.Poll(1000, SelectMode.SelectRead);
            var part2 = clientSocket.Available == 0;

            if ((part1 && part2) || !clientSocket.Connected)
            {
                return false;
            }

            return true;
        }
        private void NotifySocketDisconnected(SocketInfo socketInfo)
        {
            Task.Factory.StartNew(() =>
            {
                _logger.ErrorFormat("Socket disconnected, address:{0}", socketInfo.SocketRemotingEndpointAddress);
                _clientSocketDict.Remove(socketInfo.SocketRemotingEndpointAddress);
                if (_socketEventListener != null)
                {
                    _socketEventListener.OnSocketDisconnected(socketInfo);
                }
            });
        }
    }
}
