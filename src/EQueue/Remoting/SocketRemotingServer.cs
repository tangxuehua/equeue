using System.Collections.Generic;
using System.Threading.Tasks;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Socketing;

namespace EQueue.Remoting
{
    public class SocketRemotingServer
    {
        private readonly ServerSocket _serverSocket;
        private readonly Dictionary<int, IRequestHandler> _requestHandlerDict;
        private readonly ILogger _logger;
        private bool _started;

        public SocketRemotingServer(SocketSetting socketSetting, ISocketEventListener socketEventListener = null)
        {
            _serverSocket = new ServerSocket(socketEventListener);
            _requestHandlerDict = new Dictionary<int, IRequestHandler>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _serverSocket.Bind(socketSetting.Address, socketSetting.Port).Listen(socketSetting.Backlog);
            _started = false;
        }

        public void Start()
        {
            if (_started) return;

            Task.Factory.StartNew(() => _serverSocket.Start(HandleRemotingRequest));

            _started = true;
        }

        public void Shutdown()
        {
            _serverSocket.Shutdown();
        }

        public void RegisterRequestHandler(int requestCode, IRequestHandler requestHandler)
        {
            _requestHandlerDict[requestCode] = requestHandler;
        }

        private void HandleRemotingRequest(ReceiveContext receiveContext)
        {
            var remotingRequest = RemotingUtil.ParseRequest(receiveContext.ReceivedMessage);
            IRequestHandler requestHandler;
            if (!_requestHandlerDict.TryGetValue(remotingRequest.Code, out requestHandler))
            {
                _logger.ErrorFormat("No request handler found for remoting request, request code:{0}", remotingRequest.Code);
                return;
            }

            Task.Factory.StartNew(() =>
            {
                var remotingResponse = requestHandler.HandleRequest(new SocketRequestHandlerContext(receiveContext), remotingRequest);
                if (remotingRequest.IsOneway)
                {
                    return;
                }
                else if (remotingResponse != null)
                {
                    receiveContext.ReplyMessage = RemotingUtil.BuildResponseMessage(remotingResponse);
                    receiveContext.MessageHandledCallback(receiveContext);
                }
            });
        }
    }
}
