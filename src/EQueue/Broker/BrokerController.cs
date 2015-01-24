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
    public class BrokerController
    {
        private readonly ILogger _logger;
        private readonly IMessageService _messageService;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly SocketRemotingServer _adminSocketRemotingServer;

        public SuspendedPullRequestManager SuspendedPullRequestManager { get; private set; }
        public ConsumerManager ConsumerManager { get; private set; }

        public BrokerSetting Setting { get; private set; }

        public BrokerController() : this(null) { }
        public BrokerController(BrokerSetting setting)
        {
            Setting = setting ?? new BrokerSetting();
            SuspendedPullRequestManager = new SuspendedPullRequestManager(this);
            ConsumerManager = new ConsumerManager(this);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _producerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ProducerRemotingServer", Setting.ProducerIPEndPoint);
            _consumerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ConsumerRemotingServer", Setting.ConsumerIPEndPoint, new ConsumerSocketServerEventListener(this));
            _adminSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.AdminRemotingServer", Setting.AdminIPEndPoint);
            _messageService.SetBrokerContrller(this);
            RegisterRequestHandlers();
        }

        public BrokerController Start()
        {
            _messageService.Start();
            ConsumerManager.Start();
            SuspendedPullRequestManager.Start();
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
            ConsumerManager.Shutdown();
            SuspendedPullRequestManager.Shutdown();
            _messageService.Shutdown();
            _logger.InfoFormat("Broker shutdown, producer:[{0}], consumer:[{1}], admin:[{2}]", Setting.ProducerIPEndPoint, Setting.ConsumerIPEndPoint, Setting.AdminIPEndPoint);
            return this;
        }

        private void RegisterRequestHandlers()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler(this));
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueIdsForProducer, new GetTopicQueueIdsForProducerRequestHandler());

            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryGroupConsumer, new QueryConsumerRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueIdsForConsumer, new GetTopicQueueIdsForConsumerRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.UpdateQueueOffsetRequest, new UpdateQueueOffsetRequestHandler());

            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryTopicQueueInfo, new QueryTopicQueueInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryConsumerInfo, new QueryConsumerInfoRequestHandler(this));
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.AddQueue, new AddQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.RemoveQueue, new RemoveQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.EnableQueue, new EnableQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.DisableQueue, new DisableQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryTopicConsumeInfo, new QueryTopicConsumeInfoRequestHandler(this));
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.RemoveQueueOffsetInfo, new RemoveQueueOffsetInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryMessage, new QueryMessageRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetMessageDetail, new GetMessageDetailRequestHandler());
        }

        class ConsumerSocketServerEventListener : ISocketServerEventListener
        {
            private BrokerController _brokerController;

            public ConsumerSocketServerEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
            }

            public void OnConnectionAccepted(ITcpConnectionInfo connectionInfo)
            {
            }

            public void OnConnectionClosed(ITcpConnectionInfo connectionInfo, SocketError socketError)
            {
                _brokerController.ConsumerManager.RemoveConsumer(connectionInfo.RemoteEndPoint.ToString());
            }
        }
    }
}
