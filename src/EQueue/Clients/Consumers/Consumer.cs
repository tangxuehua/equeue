using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using System.Text;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private readonly ClientService _clientService;
        private readonly PullMessageService _pullMessageService;
        private readonly CommitConsumeOffsetService _commitConsumeOffsetService;
        private readonly RebalanceService _rebalanceService;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly IDictionary<string, HashSet<string>> _subscriptionTopics;
        private readonly ILogger _logger;
        private bool _stopped;

        #endregion

        #region Public Properties

        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public string Name { get; private set; }
        public IDictionary<string, HashSet<string>> SubscriptionTopics
        {
            get { return _subscriptionTopics; }
        }
        public bool Stopped
        {
            get { return _stopped; }
        }

        #endregion

        #region Constructors

        public Consumer(string groupName, string consumerName = null) : this(groupName, new ConsumerSetting(), consumerName) { }
        public Consumer(string groupName, ConsumerSetting setting, string consumerName = null)
        {
            if (groupName == null)
            {
                throw new ArgumentNullException("groupName");
            }

            Name = consumerName;
            GroupName = groupName;
            Setting = setting ?? new ConsumerSetting();

            if (Setting.NameServerList == null || Setting.NameServerList.Count() == 0)
            {
                throw new Exception("Name server address is not specified.");
            }

            _subscriptionTopics = new Dictionary<string, HashSet<string>>();
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
            _clientService = new ClientService(clientSetting, null, this);
            _pullMessageService = new PullMessageService(this, _clientService);
            _commitConsumeOffsetService = new CommitConsumeOffsetService(this, _clientService);
            _rebalanceService = new RebalanceService(this, _clientService, _pullMessageService, _commitConsumeOffsetService);
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
            _clientService.Start();
            _pullMessageService.Start();
            _rebalanceService.Start();
            _commitConsumeOffsetService.Start();
            _logger.InfoFormat("{0} startted.", GetType().Name);
            return this;
        }
        public Consumer Stop()
        {
            _stopped = true;
            _commitConsumeOffsetService.Stop();
            _rebalanceService.Stop();
            _pullMessageService.Stop();
            _clientService.Stop();
            _logger.InfoFormat("{0} stopped.", GetType().Name);
            return this;
        }
        public Consumer Subscribe(string topic, params string[] tags)
        {
            if (!_subscriptionTopics.ContainsKey(topic))
            {
                _subscriptionTopics.Add(topic, tags == null ? new HashSet<string>() : new HashSet<string>(tags));
            }
            else
            {
                var tagSet = _subscriptionTopics[topic];
                if (tags != null)
                {
                    foreach (var tag in tags)
                    {
                        tagSet.Add(tag);
                    }
                }
            }
            _clientService.RegisterSubscriptionTopic(topic);
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
            var brokerConnections = _clientService.GetAllBrokerConnections();
            var queueGroups = GetCurrentQueues().GroupBy(x => x.BrokerName);

            foreach (var brokerConnection in brokerConnections)
            {
                var remotingClient = brokerConnection.RemotingClient;
                var clientId = _clientService.GetClientId();

                try
                {
                    var messageQueues = new List<MessageQueueEx>();
                    var queueGroup = queueGroups.SingleOrDefault(x => x.Key == brokerConnection.BrokerInfo.BrokerName);
                    if (queueGroup != null)
                    {
                        messageQueues.AddRange(queueGroup);
                    }
                    var heartbeatData = new ConsumerHeartbeatData(clientId, GroupName, _subscriptionTopics.Keys, messageQueues);
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
