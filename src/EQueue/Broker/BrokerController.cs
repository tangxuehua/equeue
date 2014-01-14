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
        private readonly SocketRemotingServer _socketRemotingServerForProducer;
        private readonly SocketRemotingServer _socketRemotingServerForConsumer;
        private readonly SocketRemotingServer _socketRemotingServerForHeartbeat;
        private readonly ClientManager _clientManager;
        public SuspendedPullRequestManager SuspendedPullRequestManager { get; private set; }
        public ConsumerManager ConsumerManager { get; private set; }

        public BrokerController() : this(BrokerSetting.Default) { }
        public BrokerController(BrokerSetting setting)
        {
            _setting = setting;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _socketRemotingServerForHeartbeat = new SocketRemotingServer(setting.HeartbeatSocketSetting);
            _socketRemotingServerForProducer = new SocketRemotingServer(setting.ProducerSocketSetting);
            _socketRemotingServerForConsumer = new SocketRemotingServer(setting.ConsumerSocketSetting, new PullMessageSocketEventHandler(this));
            _clientManager = new ClientManager(this);
            SuspendedPullRequestManager = new SuspendedPullRequestManager();
            ConsumerManager = new ConsumerManager();
        }

        public BrokerController Initialize()
        {
            _socketRemotingServerForHeartbeat.RegisterRequestHandler((int)RequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            _socketRemotingServerForProducer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler());
            _socketRemotingServerForProducer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler(this));
            _socketRemotingServerForConsumer.RegisterRequestHandler((int)RequestCode.QueryGroupConsumer, new QueryConsumerRequestHandler(this));
            _socketRemotingServerForConsumer.RegisterRequestHandler((int)RequestCode.GetTopicQueueCount, new GetTopicQueueCountRequestHandler());
            return this;
        }
        public void Start()
        {
            _socketRemotingServerForHeartbeat.Start();
            _socketRemotingServerForProducer.Start();
            _socketRemotingServerForConsumer.Start();
            _clientManager.Start();
            SuspendedPullRequestManager.Start();
            _logger.InfoFormat("Broker started. \nSend message listening address:[{0}:{1}] \nPull message listening address:[{2}:{3}] \nHeartbeat listening address:[{4}:{5}]",
                _setting.ProducerSocketSetting.Address,
                _setting.ProducerSocketSetting.Port,
                _setting.ConsumerSocketSetting.Address,
                _setting.ConsumerSocketSetting.Port,
                _setting.HeartbeatSocketSetting.Address,
                _setting.HeartbeatSocketSetting.Port);
        }
        public void Shutdown()
        {
            _socketRemotingServerForProducer.Shutdown();
            _socketRemotingServerForConsumer.Shutdown();
            _socketRemotingServerForHeartbeat.Shutdown();
            _clientManager.Shutdown();
            SuspendedPullRequestManager.Shutdown();
        }

        class PullMessageSocketEventHandler : ISocketEventListener
        {
            private BrokerController _brokerController;

            public PullMessageSocketEventHandler(BrokerController brokerController)
            {
                _brokerController = brokerController;
            }

            public void OnNewSocketAccepted(SocketInfo socketInfo) { }

            public void OnSocketDisconnected(SocketInfo socketInfo)
            {
                _brokerController.ConsumerManager.RemoveConsumer(socketInfo.SocketRemotingEndpointAddress);
            }

            public void OnSocketReceiveException(SocketInfo socketInfo)
            {
                _brokerController.ConsumerManager.RemoveConsumer(socketInfo.SocketRemotingEndpointAddress);
            }
        }
    }
}
