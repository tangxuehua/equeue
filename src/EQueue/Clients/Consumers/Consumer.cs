using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Socketing;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicQueuesDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
        private readonly ConcurrentDictionary<string, PullRequest> _pullRequestDict = new ConcurrentDictionary<string, PullRequest>();
        private readonly List<string> _subscriptionTopics = new List<string>();
        private IList<string> _consumerIds = new List<string>();
        private readonly IScheduleService _scheduleService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly IOffsetStore _offsetStore;
        private readonly IMessageHandler _messageHandler;
        private readonly ILogger _logger;

        #endregion

        #region Public Properties

        public string Id { get; private set; }
        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public MessageModel MessageModel { get; private set; }
        public IEnumerable<string> SubscriptionTopics
        {
            get { return _subscriptionTopics; }
        }

        #endregion

        #region Constructors

        public Consumer(string groupName, MessageModel messageModel, IMessageHandler messageHandler)
            : this(ConsumerSetting.Default, groupName, messageModel, messageHandler)
        {
        }
        public Consumer(ConsumerSetting setting, string groupName, MessageModel messageModel, IMessageHandler messageHandler)
            : this(string.Format("Consumer@{0}", SocketUtils.GetLocalIPV4()), setting, groupName, messageModel, messageHandler)
        {
        }
        public Consumer(string id, ConsumerSetting setting, string groupName, MessageModel messageModel, IMessageHandler messageHandler)
        {
            Id = id;
            Setting = setting;
            GroupName = groupName;
            MessageModel = messageModel;

            _messageHandler = messageHandler;
            _remotingClient = new SocketRemotingClient(setting.BrokerAddress, setting.BrokerPort);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _offsetStore = ObjectContainer.Resolve<IOffsetStore>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        #endregion

        #region Public Methods

        public Consumer Start()
        {
            _remotingClient.Start();
            _scheduleService.ScheduleTask(Rebalance, Setting.RebalanceInterval, Setting.RebalanceInterval);
            _scheduleService.ScheduleTask(UpdateAllTopicQueues, Setting.UpdateTopicQueueCountInterval, Setting.UpdateTopicQueueCountInterval);
            _scheduleService.ScheduleTask(SendHeartbeat, Setting.HeartbeatBrokerInterval, Setting.HeartbeatBrokerInterval);
            _scheduleService.ScheduleTask(PersistOffset, Setting.PersistConsumerOffsetInterval, Setting.PersistConsumerOffsetInterval);
            _logger.InfoFormat("[{0}] started", Id);
            return this;
        }
        public Consumer Subscribe(string topic)
        {
            if (!_subscriptionTopics.Contains(topic))
            {
                _subscriptionTopics.Add(topic);
            }
            return this;
        }
        public IEnumerable<MessageQueue> GetCurrentQueues()
        {
            return _pullRequestDict.Values.Select(x => x.MessageQueue);
        }

        #endregion

        #region Private Methods

        private void Rebalance()
        {
            if (MessageModel == MessageModel.BroadCasting)
            {
                foreach (var subscriptionTopic in _subscriptionTopics)
                {
                    try
                    {
                        RebalanceBroadCasting(subscriptionTopic);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(string.Format("[{0}]: broadCasting rebalance for topic [{1}] has exception", Id, subscriptionTopic), ex);
                    }
                }
            }
            else if (MessageModel == MessageModel.Clustering)
            {
                List<string> consumerIdList;
                try
                {
                    consumerIdList = QueryGroupConsumers(GroupName).ToList();
                    if (_consumerIds.Count != consumerIdList.Count)
                    {
                        _logger.InfoFormat("[{0}]: consumerIds changed, old:{1}, new:{2}", Id, _consumerIds == null ? string.Empty : string.Join(",", _consumerIds), string.Join(",", consumerIdList));
                        _consumerIds = consumerIdList;
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("[{0}]: clustering rebalance failed as QueryGroupConsumers has exception.", Id), ex);
                    return;
                }

                consumerIdList.Sort();
                foreach (var subscriptionTopic in _subscriptionTopics)
                {
                    try
                    {
                        RebalanceClustering(subscriptionTopic, consumerIdList);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(string.Format("[{0}]: rebalanceClustering for topic [{1}] has exception", Id, subscriptionTopic), ex);
                    }
                }
            }
        }
        private void RebalanceBroadCasting(string subscriptionTopic)
        {
            IList<MessageQueue> messageQueues;
            if (_topicQueuesDict.TryGetValue(subscriptionTopic, out messageQueues))
            {
                UpdatePullRequestDict(subscriptionTopic, messageQueues);
            }
        }
        private void RebalanceClustering(string subscriptionTopic, IList<string> consumerIdList)
        {
            IList<MessageQueue> messageQueues;
            if (_topicQueuesDict.TryGetValue(subscriptionTopic, out messageQueues))
            {
                var messageQueueList = messageQueues.ToList();
                messageQueueList.Sort((x, y) =>
                {
                    if (x.QueueId > y.QueueId)
                    {
                        return 1;
                    }
                    else if (x.QueueId < y.QueueId)
                    {
                        return -1;
                    }
                    return 0;
                });

                IEnumerable<MessageQueue> allocatedMessageQueues = new List<MessageQueue>();
                try
                {
                    allocatedMessageQueues = _allocateMessageQueueStragegy.Allocate(Id, messageQueueList, consumerIdList);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("[{0}]: allocate message queue has exception.", Id), ex);
                    return;
                }

                UpdatePullRequestDict(subscriptionTopic, allocatedMessageQueues.ToList());
            }
        }
        private void UpdatePullRequestDict(string topic, IList<MessageQueue> messageQueues)
        {
            // Check message queues to remove
            var toRemovePullRequestKeys = new List<string>();
            foreach (var pullRequest in _pullRequestDict.Values.Where(x => x.MessageQueue.Topic == topic))
            {
                var key = pullRequest.MessageQueue.ToString();
                if (!messageQueues.Any(x => x.ToString() == key))
                {
                    toRemovePullRequestKeys.Add(key);
                }
            }
            foreach (var pullRequestKey in toRemovePullRequestKeys)
            {
                PullRequest pullRequest;
                if (_pullRequestDict.TryRemove(pullRequestKey, out pullRequest))
                {
                    pullRequest.Stop();
                    PersistRemovedMessageQueueOffset(pullRequest.MessageQueue);
                    _logger.InfoFormat("[{0}]: removed pull request:{1}", Id, pullRequest);
                }
            }

            // Check message queues to add.
            foreach (var messageQueue in messageQueues)
            {
                var key = messageQueue.ToString();
                PullRequest pullRequest;
                if (!_pullRequestDict.TryGetValue(key, out pullRequest))
                {
                    var request = new PullRequest(Id, GroupName, messageQueue, _remotingClient, Setting.MessageHandleMode, _messageHandler, Setting.PullRequestSetting);
                    long nextOffset = ComputePullFromWhere(messageQueue);
                    if (nextOffset >= 0)
                    {
                        request.NextOffset = nextOffset;
                        if (_pullRequestDict.TryAdd(key, request))
                        {
                            request.Start();
                            _logger.InfoFormat("[{0}]: added pull request:{1}", Id, request);
                        }
                    }
                    else
                    {
                        _logger.WarnFormat("[{0}]: the pull request {1} cannot be added as the nextOffset is < 0.", Id, request);
                    }
                }
            }
        }
        private void PersistRemovedMessageQueueOffset(MessageQueue messageQueue)
        {
            _offsetStore.Persist(messageQueue);
            _offsetStore.RemoveOffset(messageQueue);
        }
        private long ComputePullFromWhere(MessageQueue messageQueue)
        {
            var offset = -1L;

            var lastOffset = _offsetStore.ReadOffset(messageQueue, OffsetReadType.ReadFromStore);
            if (lastOffset >= 0)
            {
                offset = lastOffset;
            }
            else if (lastOffset == -1)
            {
                offset = long.MaxValue;
            }

            return offset;
        }
        private void PersistOffset()
        {
            foreach (var pullRequest in _pullRequestDict.Values)
            {
                try
                {
                    _offsetStore.Persist(pullRequest.MessageQueue);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("[{0}]: PersistOffset has exception.", Id), ex);
                }
            }
        }
        private void SendHeartbeat()
        {
            try
            {
                _remotingClient.InvokeOneway(new RemotingRequest(
                    (int)RequestCode.ConsumerHeartbeat,
                    _binarySerializer.Serialize(new ConsumerData(Id, GroupName, MessageModel, SubscriptionTopics))),
                    3000);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: SendHeartbeat has exception.", Id), ex);
            }
        }
        private void UpdateAllTopicQueues()
        {
            foreach (var topic in SubscriptionTopics)
            {
                UpdateTopicQueues(topic);
            }
        }
        private void UpdateTopicQueues(string topic)
        {
            try
            {
                var topicQueueCountFromServer = GetTopicQueueCount(topic);
                IList<MessageQueue> currentMessageQueues;
                var topicQueueCountOfLocal = _topicQueuesDict.TryGetValue(topic, out currentMessageQueues) ? currentMessageQueues.Count : 0;

                if (topicQueueCountFromServer != topicQueueCountOfLocal)
                {
                    var messageQueues = new List<MessageQueue>();
                    for (var index = 0; index < topicQueueCountFromServer; index++)
                    {
                        messageQueues.Add(new MessageQueue(topic, index));
                    }
                    _topicQueuesDict[topic] = messageQueues;
                    _logger.InfoFormat("[{0}]: topic queue count updated, topic:{1}, queueCount:{2}", Id, topic, topicQueueCountFromServer);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: updateLocalTopicQueues failed, topic:{1}", Id, topic), ex);
            }
        }
        private IEnumerable<string> QueryGroupConsumers(string groupName)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryGroupConsumer, Encoding.UTF8.GetBytes(groupName));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var consumerIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return consumerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("[{0}]: QueryGroupConsumers has exception, remoting response code:{1}", Id, remotingResponse.Code));
            }
        }
        private int GetTopicQueueCount(string topic)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.GetTopicQueueCount, Encoding.UTF8.GetBytes(topic));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return BitConverter.ToInt32(remotingResponse.Body, 0);
            }
            else
            {
                throw new Exception(string.Format("[{0}]: GetTopicQueueCount has exception, remoting response code:{1}", Id, remotingResponse.Code));
            }
        }

        #endregion
    }
}
