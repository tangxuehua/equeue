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
        private readonly Dictionary<int, IRequestProcessor> _requestProcessorDict;
        private readonly ILogger _logger;
        private bool _started;

        public SocketRemotingServer(string address = "127.0.0.1", int port = 5000, int backlog = 5000)
        {
            _serverSocket = new ServerSocket(new SocketEventHandler());
            _requestProcessorDict = new Dictionary<int, IRequestProcessor>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _serverSocket.Bind(address, port).Listen(backlog);
            _started = false;
        }

        public void Start()
        {
            if (_started) return;

            _serverSocket.Start(ProcessRemotingRequest);

            _started = true;
            _logger.InfoFormat("{0} started...", this.GetType().Name);
        }

        public void Shutdown()
        {
            _serverSocket.Shutdown();
        }

        public void RegisterRequestProcessor(int requestCode, IRequestProcessor requestProcessor)
        {
            _requestProcessorDict[requestCode] = requestProcessor;
        }

        private void ProcessRemotingRequest(ReceiveContext receiveContext)
        {
            var remotingRequest = RemotingUtil.ParseRequest(receiveContext.ReceivedMessage);
            IRequestProcessor requestProcessor;
            if (!_requestProcessorDict.TryGetValue(remotingRequest.Code, out requestProcessor))
            {
                _logger.ErrorFormat("No request processor found for request, request code:{0}", remotingRequest.Code);
                return;
            }

            Task.Factory.StartNew(() =>
            {
                var remotingResponse = requestProcessor.ProcessRequest(new SocketRequestHandlerContext(receiveContext), remotingRequest);
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

        class SocketEventHandler : ISocketEventListener
        {
            public void OnNewSocketAccepted(SocketInfo socketInfo)
            {
                //TODO
            }

            public void OnSocketDisconnected(SocketInfo socketInfo)
            {
                //TODO
            }
        }

    }
}
