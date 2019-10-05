using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private readonly PullMessageService _pullMessageService;
        private readonly CommitConsumeOffsetService _commitConsumeOffsetService;
        private readonly RebalanceService _rebalanceService;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;

        #endregion

        #region Public Properties

        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public string Name { get; private set; }
        public IDictionary<string, HashSet<string>> SubscriptionTopics { get; }
        public ClientService ClientService { get; }
        public bool Stopped { get; private set; }

        #endregion

        #region Constructors

        public Consumer(string groupName, string consumerName = "DefaultConsumer") : this(groupName, new ConsumerSetting(), consumerName) { }
        public Consumer(string groupName, ConsumerSetting setting, string consumerName = "DefaultConsumer")
        {
            Name = consumerName;
            GroupName = groupName ?? throw new ArgumentNullException("groupName");
            Setting = setting ?? new ConsumerSetting();

            if (Setting.NameServerList == null || Setting.NameServerList.Count() == 0)
            {
                throw new Exception("Name server address is not specified.");
            }

            SubscriptionTopics = new Dictionary<string, HashSet<string>>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            var clientSetting = new ClientSetting
            {
                ClientName = Name,
                ClusterName = Setting.ClusterName,
                NameServerList = Setting.NameServerList,
                SocketSetting = Setting.SocketSetting,
                OnlyFindMasterBroker = true,
                SendHeartbeatInterval = Setting.HeartbeatBrokerInterval,
                RefreshBrokerAndTopicRouteInfoInterval = Setting.RefreshBrokerAndTopicRouteInfoInterval
            };
            ClientService = new ClientService(clientSetting, null, this);
            _pullMessageService = new PullMessageService(this, ClientService);
            _commitConsumeOffsetService = new CommitConsumeOffsetService(this, ClientService);
            _rebalanceService = new RebalanceService(this, ClientService, _pullMessageService, _commitConsumeOffsetService);

            TaskScheduler.UnobservedTaskException += (sender, ex) =>
            {
                _logger.ErrorFormat("UnobservedTaskException occurred.", ex);
            };
        }

        #endregion

        #region Public Methods

        public Consumer SetMessageHandler(IMessageHandler messageHandler)
        {
            _pullMessageService.SetMessageHandler(messageHandler);
            return this;
        }
        public Consumer Start()
        {
            ClientService.Start();
            _pullMessageService.Start();
            _rebalanceService.Start();
            _commitConsumeOffsetService.Start();
            _logger.InfoFormat("{0} startted.", GetType().Name);
            return this;
        }
        public Consumer Stop()
        {
            Stopped = true;
            _commitConsumeOffsetService.Stop();
            _rebalanceService.Stop();
            _pullMessageService.Stop();
            ClientService.Stop();
            _logger.InfoFormat("{0} stopped.", GetType().Name);
            return this;
        }
        public Consumer Subscribe(string topic, params string[] tags)
        {
            if (!SubscriptionTopics.ContainsKey(topic))
            {
                SubscriptionTopics.Add(topic, tags == null ? new HashSet<string>() : new HashSet<string>(tags));
            }
            else
            {
                var tagSet = SubscriptionTopics[topic];
                if (tags != null)
                {
                    foreach (var tag in tags)
                    {
                        tagSet.Add(tag);
                    }
                }
            }
            ClientService.RegisterSubscriptionTopic(topic);
            return this;
        }
        public IEnumerable<MessageQueueEx> GetCurrentQueues()
        {
            return _rebalanceService.GetCurrentQueues();
        }
        public IEnumerable<QueueMessage> PullMessages(int maxCount, int timeoutMilliseconds, CancellationToken cancellation)
        {
            return _pullMessageService.PullMessages(maxCount, timeoutMilliseconds, cancellation);
        }
        public void CommitConsumeOffset(string brokerName, string topic, int queueId, long consumeOffset)
        {
            _commitConsumeOffsetService.CommitConsumeOffset(brokerName, topic, queueId, consumeOffset);
        }

        #endregion

        internal void SendHeartbeat()
        {
            var brokerConnections = ClientService.GetAllBrokerConnections();
            var queueGroups = GetCurrentQueues().GroupBy(x => x.BrokerName);

            foreach (var brokerConnection in brokerConnections)
            {
                var remotingClient = brokerConnection.RemotingClient;
                var clientId = ClientService.GetClientId();

                try
                {
                    var messageQueues = new List<MessageQueueEx>();
                    var queueGroup = queueGroups.SingleOrDefault(x => x.Key == brokerConnection.BrokerInfo.BrokerName);
                    if (queueGroup != null)
                    {
                        messageQueues.AddRange(queueGroup);
                    }
                    var heartbeatData = new ConsumerHeartbeatData(clientId, GroupName, SubscriptionTopics.Keys, messageQueues);
                    var json = _jsonSerializer.Serialize(heartbeatData);
                    var data = Encoding.UTF8.GetBytes(json);

                    remotingClient.InvokeOneway(new RemotingRequest((int)BrokerRequestCode.ConsumerHeartbeat, data));
                }
                catch (Exception ex)
                {
                    if (remotingClient.IsConnected)
                    {
                        _logger.Error(string.Format("Send consumer heartbeat has exception, brokerInfo: {0}", brokerConnection.BrokerInfo), ex);
                    }
                }
            }
        }
    }
}
