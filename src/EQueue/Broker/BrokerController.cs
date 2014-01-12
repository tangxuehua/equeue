using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Processors;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Socketing;
using EQueue.Remoting;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private readonly IMessageService _messageService;
        private readonly SocketRemotingServer _sendMessageRemotingServer;
        private readonly SocketRemotingServer _pullMessageRemotingServer;
        private readonly SocketRemotingServer _heartbeatRemotingServer;
        private readonly ClientManager _clientManager;
        public SuspendedPullRequestManager SuspendedPullRequestManager { get; private set; }
        public ProducerManager ProducerManager { get; private set; }
        public ConsumerManager ConsumerManager { get; private set; }

        public BrokerController() : this(BrokerSetting.Default) { }
        public BrokerController(BrokerSetting borkerSetting)
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _sendMessageRemotingServer = new SocketRemotingServer(borkerSetting.SendMessageSocketSetting);
            _pullMessageRemotingServer = new SocketRemotingServer(borkerSetting.PullMessageSocketSetting, new PullMessageSocketEventHandler(this));
            _heartbeatRemotingServer = new SocketRemotingServer(borkerSetting.HeartbeatSocketSetting);
            _clientManager = new ClientManager(this);
            SuspendedPullRequestManager = new SuspendedPullRequestManager();
            ProducerManager = new ProducerManager();
            ConsumerManager = new ConsumerManager();
        }

        public BrokerController Initialize()
        {
            _sendMessageRemotingServer.RegisterRequestProcessor((int)RequestCode.SendMessage, new SendMessageRequestProcessor());
            _sendMessageRemotingServer.RegisterRequestProcessor((int)RequestCode.PullMessage, new PullMessageRequestProcessor(this));
            return this;
        }
        public void Start()
        {
            _sendMessageRemotingServer.Start();
            _pullMessageRemotingServer.Start();
            _heartbeatRemotingServer.Start();
            _clientManager.Start();
            SuspendedPullRequestManager.Start();
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
