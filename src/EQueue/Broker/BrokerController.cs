using System;
using System.Net.Sockets;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Socketing;
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
        private readonly ClientManager _clientManager;

        public SuspendedPullRequestManager SuspendedPullRequestManager { get; private set; }
        public ConsumerManager ConsumerManager { get; private set; }

        public BrokerSetting Setting { get; private set; }

        public BrokerController() : this(null) { }
        public BrokerController(BrokerSetting setting)
        {
            Setting = setting ?? new BrokerSetting();
            SuspendedPullRequestManager = new SuspendedPullRequestManager();
            ConsumerManager = new ConsumerManager();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _producerSocketRemotingServer = new SocketRemotingServer("ProducerRemotingServer", Setting.ProducerSocketSetting, new ProducerSocketEventListener(this));
            _consumerSocketRemotingServer = new SocketRemotingServer("ConsumerRemotingServer", Setting.ConsumerSocketSetting, new ConsumerSocketEventListener(this));
            _clientManager = new ClientManager(this);
            _messageService.SetBrokerContrller(this);
        }

        public BrokerController Initialize()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler(this));
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueCount, new GetTopicQueueCountRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryGroupConsumer, new QueryConsumerRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueCount, new GetTopicQueueCountRequestHandler());
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.UpdateQueueOffsetRequest, new UpdateQueueOffsetRequestHandler());
            return this;
        }
        public BrokerController Start()
        {
            _producerSocketRemotingServer.Start();
            _consumerSocketRemotingServer.Start();
            _clientManager.Start();
            SuspendedPullRequestManager.Start();
            _messageService.Start();
            _logger.InfoFormat("Broker started, producer:[{0}:{1}], consumer:[{2}:{3}]",
                Setting.ProducerSocketSetting.Address,
                Setting.ProducerSocketSetting.Port,
                Setting.ConsumerSocketSetting.Address,
                Setting.ConsumerSocketSetting.Port);
            return this;
        }
        public BrokerController Shutdown()
        {
            _producerSocketRemotingServer.Shutdown();
            _consumerSocketRemotingServer.Shutdown();
            _clientManager.Shutdown();
            SuspendedPullRequestManager.Shutdown();
            _messageService.Shutdown();
            _logger.InfoFormat("Broker shutdown, producer:[{0}:{1}], consumer:[{2}:{3}]",
                Setting.ProducerSocketSetting.Address,
                Setting.ProducerSocketSetting.Port,
                Setting.ConsumerSocketSetting.Address,
                Setting.ConsumerSocketSetting.Port);
            return this;
        }

        class ProducerSocketEventListener : ISocketEventListener
        {
            private readonly ILogger _logger;
            private BrokerController _brokerController;

            public ProducerSocketEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("EQueue.Broker.ProducerSocketEventListener");
            }

            public void OnNewSocketAccepted(SocketInfo socketInfo)
            {
                _logger.DebugFormat("Accepted new producer, address:{0}", socketInfo.SocketRemotingEndpointAddress);
            }

            public void OnSocketException(SocketInfo socketInfo, SocketException socketException)
            {
                if (SocketUtils.IsSocketDisconnectedException(socketException))
                {
                    _logger.DebugFormat("Producer disconnected, address:{0}", socketInfo.SocketRemotingEndpointAddress);
                }
                else
                {
                    _logger.ErrorFormat("Producer SocketException, address:{0}, errorCode:{1}", socketInfo.SocketRemotingEndpointAddress, socketException.SocketErrorCode);
                }
            }
        }
        class ConsumerSocketEventListener : ISocketEventListener
        {
            private readonly ILogger _logger;
            private BrokerController _brokerController;

            public ConsumerSocketEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
                _logger = ObjectContainer.Resolve<ILoggerFactory>().Create("EQueue.Broker.ConsumerSocketEventListener");
            }

            public void OnNewSocketAccepted(SocketInfo socketInfo)
            {
                _logger.DebugFormat("Accepted new consumer, address:{0}", socketInfo.SocketRemotingEndpointAddress);
            }

            public void OnSocketException(SocketInfo socketInfo, SocketException socketException)
            {
                if (SocketUtils.IsSocketDisconnectedException(socketException))
                {
                    _brokerController.ConsumerManager.RemoveConsumer(socketInfo.SocketRemotingEndpointAddress);
                }
                else
                {
                    _logger.ErrorFormat("Consumer SocketException, address:{0}, errorCode:{1}", socketInfo.SocketRemotingEndpointAddress, socketException.SocketErrorCode);
                }
            }
        }
    }
}
