using System;
using System.Linq;
using System.Net.Sockets;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Socketing;
using ECommon.TcpTransport;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Processors;
using EQueue.Protocols;

namespace EQueue.Broker
{
    public class BrokerController : ISocketServerEventListener
    {
        private static BrokerController _instance;
        private readonly ILogger _logger;
        private readonly IQueueService _queueService;
        private readonly IMessageService _messageService;
        private readonly IMessageStore _messageStore;
        private readonly IOffsetManager _offsetManager;
        private readonly ConsumerManager _consumerManager;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly SocketRemotingServer _adminSocketRemotingServer;
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;

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
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _queueService = ObjectContainer.Resolve<IQueueService>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _producerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ProducerRemotingServer", Setting.ProducerIPEndPoint);
            _consumerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ConsumerRemotingServer", Setting.ConsumerIPEndPoint, this);
            _adminSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.AdminRemotingServer", Setting.AdminIPEndPoint);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            RegisterRequestHandlers();
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
            _queueService.Start();
            _offsetManager.Start();
            _messageService.Start();
            _consumerManager.Start();
            _suspendedPullRequestManager.Start();
            _consumerSocketRemotingServer.Start();
            _producerSocketRemotingServer.Start();
            _adminSocketRemotingServer.Start();
            _logger.InfoFormat("Broker started, producer:[{0}], consumer:[{1}], admin:[{2}]", Setting.ProducerIPEndPoint, Setting.ConsumerIPEndPoint, Setting.AdminIPEndPoint);
            return this;
        }
        public BrokerController Shutdown()
        {
            _producerSocketRemotingServer.Shutdown();
            _consumerSocketRemotingServer.Shutdown();
            _adminSocketRemotingServer.Shutdown();
            _consumerManager.Shutdown();
            _suspendedPullRequestManager.Shutdown();
            _queueService.Shutdown();
            _messageService.Shutdown();
            _offsetManager.Shutdown();
            _logger.InfoFormat("Broker shutdown, producer:[{0}], consumer:[{1}], admin:[{2}]", Setting.ProducerIPEndPoint, Setting.ConsumerIPEndPoint, Setting.AdminIPEndPoint);
            return this;
        }
        public BrokerStatisticInfo GetBrokerStatisticInfo()
        {
            var statisticInfo = new BrokerStatisticInfo();
            statisticInfo.TopicCount = _queueService.GetAllTopics().Count();
            statisticInfo.QueueCount = _queueService.GetAllQueueCount();
            statisticInfo.UnConsumedQueueMessageCount = _queueService.GetAllQueueUnConusmedMessageCount();
            statisticInfo.InMemoryQueueMessageCount = _queueService.GetAllQueueIndexCount();
            statisticInfo.CurrentMessageOffset = _messageStore.CurrentMessageOffset;
            statisticInfo.PersistedMessageOffset = _messageStore.PersistedMessageOffset;
            statisticInfo.MinMessageOffset = _queueService.GetQueueMinMessageOffset();
            statisticInfo.ConsumerGroupCount = _offsetManager.GetConsumerGroupCount();
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
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryMessage, new QueryMessageRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetMessageDetail, new GetMessageDetailRequestHandler());
        }

        void ISocketServerEventListener.OnConnectionAccepted(ITcpConnectionInfo connectionInfo) { }
        void ISocketServerEventListener.OnConnectionClosed(ITcpConnectionInfo connectionInfo, SocketError socketError)
        {
            _consumerManager.RemoveConsumer(connectionInfo.RemoteEndPoint.ToString());
        }
    }
}
