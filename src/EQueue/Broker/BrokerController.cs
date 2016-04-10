using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Socketing;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.RequestHandlers;
using EQueue.Broker.RequestHandlers.Admin;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private static BrokerController _instance;
        private readonly ILogger _logger;
        private readonly IQueueStore _queueStore;
        private readonly IMessageStore _messageStore;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly ProducerManager _producerManager;
        private readonly ConsumerManager _consumerManager;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly SocketRemotingServer _adminSocketRemotingServer;
        private readonly ConsoleEventHandlerService _service;
        private readonly IChunkStatisticService _chunkReadStatisticService;
        private int _isShuttingdown = 0;
        private int _isCleaning = 0;

        public BrokerSetting Setting { get; private set; }
        public ProducerManager ProducerManager
        {
            get { return _producerManager; }
        }
        public ConsumerManager ConsumerManager
        {
            get { return _consumerManager; }
        }
        public bool IsCleaning
        {
            get { return _isCleaning == 1; }
        }
        public static BrokerController Instance
        {
            get { return _instance; }
        }

        private BrokerController(BrokerSetting setting)
        {
            Setting = setting ?? new BrokerSetting();
            _producerManager = ObjectContainer.Resolve<ProducerManager>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _consumeOffsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _chunkReadStatisticService = ObjectContainer.Resolve<IChunkStatisticService>();

            _producerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ProducerRemotingServer", Setting.ProducerAddress, Setting.SocketSetting);
            _consumerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ConsumerRemotingServer", Setting.ConsumerAddress, Setting.SocketSetting);
            _adminSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.AdminRemotingServer", Setting.AdminAddress, Setting.SocketSetting);

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _producerSocketRemotingServer.RegisterConnectionEventListener(new ProducerConnectionEventListener(this));
            _consumerSocketRemotingServer.RegisterConnectionEventListener(new ConsumerConnectionEventListener(this));
            RegisterRequestHandlers();

            _service = new ConsoleEventHandlerService();
            _service.RegisterClosingEventHandler(eventCode => { Shutdown(); });
        }

        public static BrokerController Create(BrokerSetting setting = null)
        {
            _instance = new BrokerController(setting);
            return _instance;
        }
        public BrokerController Clean()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("Broker clean starting...");

            if (Interlocked.CompareExchange(ref _isCleaning, 1, 0) == 0)
            {
                try
                {
                    //首先关闭所有组件
                    _queueStore.Shutdown();
                    _consumeOffsetStore.Shutdown();
                    _messageStore.Shutdown();
                    _suspendedPullRequestManager.Clean();

                    //再删除Broker的整个存储目录以及目录下的所有文件
                    if (Directory.Exists(Setting.FileStoreRootPath))
                    {
                        Directory.Delete(Setting.FileStoreRootPath, true);
                    }

                    //再重新加载和启动所有组件
                    _messageStore.Load();
                    _queueStore.Load();

                    _consumeOffsetStore.Start();
                    _messageStore.Start();
                    _queueStore.Start();

                    Interlocked.Exchange(ref _isCleaning, 0);
                    _logger.InfoFormat("Broker clean success, timeSpent:{0}ms, producer:[{1}], consumer:[{2}], admin:[{3}]", watch.ElapsedMilliseconds, Setting.ProducerAddress, Setting.ConsumerAddress, Setting.AdminAddress);
                }
                catch (Exception ex)
                {
                    _logger.ErrorFormat("Broker clean failed.", ex);
                    throw;
                }
            }

            return this;
        }
        public BrokerController Start()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("Broker starting...");

            _messageStore.Load();
            _queueStore.Load();

            if (_messageStore.ChunkCount == 0 || _queueStore.GetAllQueueCount() == 0)
            {
                _logger.InfoFormat("The message store or queue store is empty, try to clear all the broker store files.");

                _messageStore.Shutdown();
                _queueStore.Shutdown();

                if (Directory.Exists(Setting.FileStoreRootPath))
                {
                    Directory.Delete(Setting.FileStoreRootPath, true);
                }

                _logger.InfoFormat("All the broker store files clear success.");

                _messageStore.Load();
                _queueStore.Load();
            }

            _consumeOffsetStore.Start();
            _messageStore.Start();
            _queueStore.Start();
            _producerManager.Start();
            _consumerManager.Start();
            _suspendedPullRequestManager.Start();
            _consumerSocketRemotingServer.Start();
            _producerSocketRemotingServer.Start();
            _adminSocketRemotingServer.Start();
            _chunkReadStatisticService.Start();

            RemoveNotExistQueueConsumeOffsets();

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
                _producerManager.Shutdown();
                _consumerManager.Shutdown();
                _suspendedPullRequestManager.Shutdown();
                _messageStore.Shutdown();
                _consumeOffsetStore.Shutdown();
                _queueStore.Shutdown();
                _chunkReadStatisticService.Shutdown();
                _logger.InfoFormat("Broker shutdown success, timeSpent:{0}ms", watch.ElapsedMilliseconds);
            }
            return this;
        }
        public BrokerStatisticInfo GetBrokerStatisticInfo()
        {
            var statisticInfo = new BrokerStatisticInfo();
            statisticInfo.TopicCount = _queueStore.GetAllTopics().Count();
            statisticInfo.QueueCount = _queueStore.GetAllQueueCount();
            statisticInfo.TotalUnConsumedMessageCount = _queueStore.GetTotalUnConusmedMessageCount();
            statisticInfo.ConsumerGroupCount = _consumerManager.GetConsumerGroupCount();
            statisticInfo.ProducerCount = _producerManager.GetProducerCount();
            statisticInfo.ConsumerCount = _consumerManager.GetConsumerCount();
            statisticInfo.MessageChunkCount = _messageStore.ChunkCount;
            statisticInfo.MessageMinChunkNum = _messageStore.MinChunkNum;
            statisticInfo.MessageMaxChunkNum = _messageStore.MaxChunkNum;
            return statisticInfo;
        }

        private void RemoveNotExistQueueConsumeOffsets()
        {
            var consumeKeys = _consumeOffsetStore.GetConsumeKeys();
            foreach (var consumeKey in consumeKeys)
            {
                if (!_queueStore.IsQueueExist(consumeKey))
                {
                    _consumeOffsetStore.DeleteConsumeOffset(consumeKey);
                }
            }
        }
        private void RegisterRequestHandlers()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.ProducerHeartbeat, new ProducerHeartbeatRequestHandler(this));
            _producerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SendMessage, new SendMessageRequestHandler(this));

            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)RequestCode.PullMessage, new PullMessageRequestHandler());

            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueIdsForProducer, new GetTopicQueueIdsForProducerRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetTopicQueueIdsForConsumer, new GetTopicQueueIdsForConsumerRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryGroupConsumer, new QueryConsumerRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.UpdateQueueOffsetRequest, new UpdateQueueOffsetRequestHandler());

            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryBrokerStatisticInfo, new QueryBrokerStatisticInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.CreateTopic, new CreateTopicRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.DeleteTopic, new DeleteTopicRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryTopicQueueInfo, new QueryTopicQueueInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryProducerInfo, new QueryProducerInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.QueryConsumerInfo, new QueryConsumerInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.AddQueue, new AddQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.DeleteQueue, new DeleteQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SetProducerVisible, new SetQueueProducerVisibleRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SetConsumerVisible, new SetQueueConsumerVisibleRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.GetMessageDetail, new GetMessageDetailRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)RequestCode.SetQueueNextConsumeOffset, new SetQueueNextConsumeOffsetRequestHandler());
        }

        class ProducerConnectionEventListener : IConnectionEventListener
        {
            private BrokerController _brokerController;

            public ProducerConnectionEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
            }

            public void OnConnectionAccepted(ITcpConnection connection) { }
            public void OnConnectionEstablished(ITcpConnection connection) { }
            public void OnConnectionFailed(SocketError socketError) { }
            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                var producerId = ClientIdFactory.CreateClientId(connection.RemotingEndPoint as IPEndPoint);
                _brokerController._producerManager.RemoveProducer(producerId);
            }
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
                var consumerId = ClientIdFactory.CreateClientId(connection.RemotingEndPoint as IPEndPoint);
                if (_brokerController.Setting.RemoveConsumerWhenDisconnect)
                {
                    _brokerController._consumerManager.RemoveConsumer(consumerId);
                }
            }
        }
    }
}
