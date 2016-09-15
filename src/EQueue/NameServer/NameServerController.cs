using System.Diagnostics;
using System.Net.Sockets;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Socketing;
using EQueue.NameServer.RequestHandlers;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.NameServer
{
    public class NameServerController
    {
        private readonly ILogger _logger;
        private readonly SocketRemotingServer _socketRemotingServer;
        private readonly ConsoleEventHandlerService _service;
        private int _isShuttingdown = 0;

        public NameServerSetting Setting { get; private set; }
        public RouteInfoManager RouteInfoManager { get; private set; }

        public NameServerController(NameServerSetting setting = null)
        {
            Setting = setting ?? new NameServerSetting();
            RouteInfoManager = new RouteInfoManager(this);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _socketRemotingServer = new SocketRemotingServer("EQueue.NameServer.RemotingServer", Setting.BindingAddress, Setting.SocketSetting);
            _service = new ConsoleEventHandlerService();
            _service.RegisterClosingEventHandler(eventCode => { Shutdown(); });
            _socketRemotingServer.RegisterConnectionEventListener(new BrokerConnectionEventListener(this));
            RegisterRequestHandlers();
        }

        public NameServerController Start()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("NameServer starting...");
            RouteInfoManager.Start();
            _socketRemotingServer.Start();
            Interlocked.Exchange(ref _isShuttingdown, 0);
            _logger.InfoFormat("NameServer started, timeSpent: {0}ms, bindingAddress: {1}", watch.ElapsedMilliseconds, Setting.BindingAddress);
            return this;
        }
        public NameServerController Shutdown()
        {
            if (Interlocked.CompareExchange(ref _isShuttingdown, 1, 0) == 0)
            {
                var watch = Stopwatch.StartNew();
                _logger.InfoFormat("NameServer starting to shutdown, bindingAddress: {0}", Setting.BindingAddress);
                _socketRemotingServer.Shutdown();
                RouteInfoManager.Shutdown();
                _logger.InfoFormat("NameServer shutdown success, timeSpent: {0}ms", watch.ElapsedMilliseconds);
            }
            return this;
        }

        private void RegisterRequestHandlers()
        {
            _socketRemotingServer.RegisterRequestHandler((int)RequestCode.GetAllClusters, new GetAllClustersRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)RequestCode.RegisterBroker, new RegisterBrokerRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)RequestCode.UnregisterBroker, new UnregisterBrokerRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)RequestCode.GetClusterBrokers, new GetClusterBrokersRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicRouteInfo, new GetTopicRouteInfoRequestHandler(this));
        }

        class BrokerConnectionEventListener : IConnectionEventListener
        {
            private NameServerController _nameServerController;

            public BrokerConnectionEventListener(NameServerController nameServerController)
            {
                _nameServerController = nameServerController;
            }

            public void OnConnectionAccepted(ITcpConnection connection) { }
            public void OnConnectionEstablished(ITcpConnection connection) { }
            public void OnConnectionFailed(SocketError socketError) { }
            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                _nameServerController.RouteInfoManager.UnregisterBroker(connection.RemotingEndPoint);
            }
        }
    }
}
