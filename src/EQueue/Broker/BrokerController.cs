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
        private readonly SocketRemotingServer _sendMessageRemotingServer;
        private readonly SocketRemotingServer _pullMessageRemotingServer;
        private readonly SocketRemotingServer _heartbeatRemotingServer;
        private readonly ClientManager _clientManager;
        public SuspendedPullRequestManager SuspendedPullRequestManager { get; private set; }
        public ProducerManager ProducerManager { get; private set; }
        public ConsumerManager ConsumerManager { get; private set; }

        public BrokerController() : this(BrokerSetting.Default) { }
        public BrokerController(BrokerSetting setting)
        {
            _setting = setting;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _sendMessageRemotingServer = new SocketRemotingServer(setting.SendMessageSocketSetting);
            _pullMessageRemotingServer = new SocketRemotingServer(setting.PullMessageSocketSetting, new PullMessageSocketEventHandler(this));
            _heartbeatRemotingServer = new SocketRemotingServer(setting.HeartbeatSocketSetting);
            _clientManager = new ClientManager(this);
            SuspendedPullRequestManager = new SuspendedPullRequestManager();
            ProducerManager = new ProducerManager();
            ConsumerManager = new ConsumerManager();
        }

        public BrokerController Initialize()
        {
            _sendMessageRemotingServer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler());
            _sendMessageRemotingServer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler(this));
            return this;
        }
        public void Start()
        {
            _sendMessageRemotingServer.Start();
            _pullMessageRemotingServer.Start();
            _heartbeatRemotingServer.Start();
            _clientManager.Start();
            SuspendedPullRequestManager.Start();
            _logger.InfoFormat("Broker started. \nSend message listening address:[{0}:{1}] \nPull message listening address:[{2}:{3}] \nHeartbeat listening address:[{4}:{5}]",
                _setting.SendMessageSocketSetting.Address,
                _setting.SendMessageSocketSetting.Port,
                _setting.PullMessageSocketSetting.Address,
                _setting.PullMessageSocketSetting.Port,
                _setting.HeartbeatSocketSetting.Address,
                _setting.HeartbeatSocketSetting.Port);
        }
        public void Shutdown()
        {
            _sendMessageRemotingServer.Shutdown();
            _pullMessageRemotingServer.Shutdown();
            _heartbeatRemotingServer.Shutdown();
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
