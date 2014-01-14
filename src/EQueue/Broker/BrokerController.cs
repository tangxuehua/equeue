using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Processors;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Socketing;
using EQueue.Remoting;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private readonly BrokerSetting _setting;
        private readonly ILogger _logger;
        private readonly IMessageService _messageService;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly ClientManager _clientManager;
        public SuspendedPullRequestManager SuspendedPullRequestManager { get; private set; }
        public ConsumerManager ConsumerManager { get; private set; }

        public BrokerController() : this(BrokerSetting.Default) { }
        public BrokerController(BrokerSetting setting)
        {
            _setting = setting;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _producerSocketRemotingServer = new SocketRemotingServer(setting.ProducerSocketSetting);
            _consumerSocketRemotingServer = new SocketRemotingServer(setting.ConsumerSocketSetting, new ConsumerSocketEventHandler(this));
            _clientManager = new ClientManager(this);
            SuspendedPullRequestManager = new SuspendedPullRequestManager();
            ConsumerManager = new ConsumerManager();
        }

        public BrokerController Initialize()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryGroupConsumer, new QueryConsumerRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueCount, new GetTopicQueueCountRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            return this;
        }
        public void Start()
        {
            _producerSocketRemotingServer.Start();
            _consumerSocketRemotingServer.Start();
            _clientManager.Start();
            SuspendedPullRequestManager.Start();
            _logger.InfoFormat("Broker started, listening address for producer:[{0}:{1}], listening address for consumer:[{2}:{3}]]",
                _setting.ProducerSocketSetting.Address,
                _setting.ProducerSocketSetting.Port,
                _setting.ConsumerSocketSetting.Address,
                _setting.ConsumerSocketSetting.Port);
        }
        public void Shutdown()
        {
            _producerSocketRemotingServer.Shutdown();
            _consumerSocketRemotingServer.Shutdown();
            _clientManager.Shutdown();
            SuspendedPullRequestManager.Shutdown();
        }

        class ConsumerSocketEventHandler : ISocketEventListener
        {
            private readonly ILogger _logger;
            private BrokerController _brokerController;

            public ConsumerSocketEventHandler(BrokerController brokerController)
            {
                _brokerController = brokerController;
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            }

            public void OnNewSocketAccepted(SocketInfo socketInfo)
            {
                _logger.InfoFormat("Accepted new consumer, address:{0}", socketInfo.SocketRemotingEndpointAddress);
            }

            public void OnSocketDisconnected(SocketInfo socketInfo)
            {
                _brokerController.ConsumerManager.RemoveConsumer(socketInfo.SocketRemotingEndpointAddress);
                _logger.InfoFormat("Consumer disconnected, address:{0}", socketInfo.SocketRemotingEndpointAddress);
            }

            public void OnSocketReceiveException(SocketInfo socketInfo)
            {
                _brokerController.ConsumerManager.RemoveConsumer(socketInfo.SocketRemotingEndpointAddress);
                _logger.InfoFormat("Consumer exception, address:{0}", socketInfo.SocketRemotingEndpointAddress);
            }
        }
    }
}
