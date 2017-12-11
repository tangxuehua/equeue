using System.Diagnostics;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Utilities;
using EQueue.NameServer.RequestHandlers;
using EQueue.Protocols.NameServers;

namespace EQueue.NameServer
{
    public class NameServerController
    {
        private readonly ILogger _logger;
        private readonly SocketRemotingServer _socketRemotingServer;
        private int _isShuttingdown = 0;

        public NameServerSetting Setting { get; private set; }
        public ClusterManager ClusterManager { get; private set; }

        public NameServerController(NameServerSetting setting = null)
        {
            Setting = setting ?? new NameServerSetting();
            ClusterManager = new ClusterManager(this);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _socketRemotingServer = new SocketRemotingServer("EQueue.NameServer.RemotingServer", Setting.BindingAddress, Setting.SocketSetting);
            RegisterRequestHandlers();
        }

        public NameServerController Start()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("NameServer starting...");
            ClusterManager.Start();
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
                ClusterManager.Shutdown();
                _logger.InfoFormat("NameServer shutdown success, timeSpent: {0}ms", watch.ElapsedMilliseconds);
            }
            return this;
        }

        private void RegisterRequestHandlers()
        {
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.RegisterBroker, new RegisterBrokerRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.UnregisterBroker, new UnregisterBrokerRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetAllClusters, new GetAllClustersRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetClusterBrokers, new GetClusterBrokersRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetClusterBrokerStatusInfoList, new GetClusterBrokerStatusInfoListRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicRouteInfo, new GetTopicRouteInfoRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicQueueInfo, new GetTopicQueueInfoRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicConsumeInfo, new GetTopicConsumeInfoRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetProducerList, new GetProducerListRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetConsumerList, new GetConsumerListRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.CreateTopic, new CreateTopicForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.DeleteTopic, new DeleteTopicForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.AddQueue, new AddQueueForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.DeleteQueue, new DeleteQueueForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.SetQueueProducerVisible, new SetQueueProducerVisibleForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.SetQueueConsumerVisible, new SetQueueConsumerVisibleForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.SetQueueNextConsumeOffset, new SetQueueNextConsumeOffsetForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.DeleteConsumerGroup, new DeleteConsumerGroupForClusterRequestHandler(this));
            _socketRemotingServer.RegisterRequestHandler((int)NameServerRequestCode.GetTopicAccumulateInfoList, new GetTopicAccumulateInfoListRequestHandler(this));
        }
    }
}
