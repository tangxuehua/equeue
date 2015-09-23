using System;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Socketing;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Processors;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private static BrokerController _instance;
        private readonly ILogger _logger;
        private readonly IQueueStore _queueService;
        private readonly IMessageStore _messageStore;
        private readonly IOffsetStore _offsetStore;
        private readonly ConsumerManager _consumerManager;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly SocketRemotingServer _adminSocketRemotingServer;
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly ConsoleEventHandlerService _service;
        private int _isShuttingdown = 0;

        public BrokerSetting Setting { get; private set; }
        public static BrokerController Instance
        {
            get { return _instance; }
        }

        private BrokerController(BrokerSetting setting)
        {
            Setting = setting ?? new BrokerSetting();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _offsetStore = ObjectContainer.Resolve<IOffsetStore>();
            _queueService = ObjectContainer.Resolve<IQueueStore>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();

            _producerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ProducerRemotingServer", Setting.ProducerAddress);
            _consumerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ConsumerRemotingServer", Setting.ConsumerAddress);
            _adminSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.AdminRemotingServer", Setting.AdminAddress);

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _consumerSocketRemotingServer.RegisterConnectionEventListener(new ConsumerConnectionEventListener(this));
            RegisterRequestHandlers();

            _service = new ConsoleEventHandlerService();
            _service.RegisterClosingEventHandler(eventCode => { Shutdown(); });
        }

        public static BrokerController Create(BrokerSetting setting = null)
        {
            if (_instance != null)
            {
                throw new NotSupportedException("Broker controller cannot be create twice.");
            }
            _instance = new BrokerController(setting);
            return _instance;
        }
        public BrokerController Start()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("Broker starting...");
            _queueService.Start();
            _offsetStore.Start();
            _messageStore.Start();
            _consumerManager.Start();
            _suspendedPullRequestManager.Start();
            _consumerSocketRemotingServer.Start();
            _producerSocketRemotingServer.Start();
            _adminSocketRemotingServer.Start();
            Interlocked.Exchange(ref _isShuttingdown, 0);
            _logger.InfoFormat("Broker started, timeSpent:{0}ms, producer:[{1}], consumer:[{2}], admin:[{3}]", watch.ElapsedMilliseconds, Setting.ProducerAddress, Setting.ConsumerAddress, Setting.AdminAddress);
            return this;
        }
        public BrokerController Shutdown()
        {
            if (Interlocked.CompareExchange(ref _isShuttingdown, 1, 0) == 0)
            {
                var watch = Stopwatch.StartNew();
                _logger.InfoFormat("Broker starting to shutdown, producer:[{0}], consumer:[{1}], admin:[{2}]", Setting.ProducerAddress, Setting.ConsumerAddress, Setting.AdminAddress);
                _producerSocketRemotingServer.Shutdown();
                _consumerSocketRemotingServer.Shutdown();
                _adminSocketRemotingServer.Shutdown();
                _consumerManager.Shutdown();
                _suspendedPullRequestManager.Shutdown();
                _messageStore.Shutdown();
                _offsetStore.Shutdown();
                _queueService.Shutdown();
                _logger.InfoFormat("Broker shutdown success, timeSpent:{0}ms", watch.ElapsedMilliseconds);
            }
            return this;
        }
        public BrokerStatisticInfo GetBrokerStatisticInfo()
        {
            var statisticInfo = new BrokerStatisticInfo();
            statisticInfo.TopicCount = _queueService.GetAllTopics().Count();
            statisticInfo.QueueCount = _queueService.GetAllQueueCount();
            statisticInfo.UnConsumedQueueMessageCount = _queueService.GetAllQueueUnConusmedMessageCount();
            statisticInfo.CurrentMessageOffset = _messageStore.CurrentMessagePosition;
            statisticInfo.MinMessageOffset = _messageStore.MinMessagePosition;
            statisticInfo.ConsumerGroupCount = _offsetStore.GetConsumerGroupCount();
            return statisticInfo;
        }

        private void RegisterRequestHandlers()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler());
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueIdsForProducer, new GetTopicQueueIdsForProducerRequestHandler());

            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryGroupConsumer, new QueryConsumerRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueIdsForConsumer, new GetTopicQueueIdsForConsumerRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.UpdateQueueOffsetRequest, new UpdateQueueOffsetRequestHandler());

            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryBrokerStatisticInfo, new QueryBrokerStatisticInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.CreateTopic, new CreateTopicRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryTopicQueueInfo, new QueryTopicQueueInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryConsumerInfo, new QueryConsumerInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.AddQueue, new AddQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.RemoveQueue, new RemoveQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.EnableQueue, new EnableQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.DisableQueue, new DisableQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryTopicConsumeInfo, new QueryTopicConsumeInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.RemoveQueueOffsetInfo, new RemoveQueueOffsetInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetMessageDetail, new GetMessageDetailRequestHandler());
        }

        class ConsumerConnectionEventListener : IConnectionEventListener
        {
            private BrokerController _brokerController;

            public ConsumerConnectionEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
            }

            public void OnConnectionAccepted(ITcpConnection connection) { }
            public void OnConnectionEstablished(ITcpConnection connection) { }
            public void OnConnectionFailed(SocketError socketError) { }
            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                _brokerController._consumerManager.RemoveConsumer(connection.RemotingEndPoint.ToString());
            }
        }
    }
}
