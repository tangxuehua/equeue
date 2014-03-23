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
using ECommon.Utilities;
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
        private readonly List<int> _taskIds = new List<int>();
        private readonly IScheduleService _scheduleService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly ILocalOffsetStore _localOffsetStore;
        private readonly ILogger _logger;
        private IList<string> _consumerIds = new List<string>();
        private IMessageHandler _messageHandler;

        #endregion

        #region Public Properties

        public string Id { get; private set; }
        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public IEnumerable<string> SubscriptionTopics
        {
            get { return _subscriptionTopics; }
        }

        #endregion

        #region Constructors

        public Consumer(string groupName)
            : this(new ConsumerSetting(), null, groupName)
        {
        }
        public Consumer(string name, string groupName)
            : this(new ConsumerSetting(), name, groupName)
        {
        }
        public Consumer(ConsumerSetting setting, string name, string groupName)
            : this(string.Format("{0}@{1}@{2}", SocketUtils.GetLocalIPV4(), string.IsNullOrEmpty(name) ? typeof(Consumer).Name : name, ObjectId.GenerateNewId()), setting, groupName)
        {
        }
        public Consumer(string id, ConsumerSetting setting, string groupName)
        {
            Id = id;
            Setting = setting ?? new ConsumerSetting();
            GroupName = groupName;
            _remotingClient = new SocketRemotingClient(Setting.BrokerAddress, Setting.BrokerPort);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _localOffsetStore = ObjectContainer.Resolve<ILocalOffsetStore>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        #endregion

        #region Public Methods

        public Consumer Start(IMessageHandler messageHandler)
        {
            _messageHandler = messageHandler;
            _remotingClient.Start();

            _taskIds.Add(_scheduleService.ScheduleTask(Rebalance, Setting.RebalanceInterval, Setting.RebalanceInterval));
            _taskIds.Add(_scheduleService.ScheduleTask(UpdateAllTopicQueues, Setting.UpdateTopicQueueCountInterval, Setting.UpdateTopicQueueCountInterval));
            _taskIds.Add(_scheduleService.ScheduleTask(SendHeartbeat, Setting.HeartbeatBrokerInterval, Setting.HeartbeatBrokerInterval));
            _taskIds.Add(_scheduleService.ScheduleTask(PersistOffset, Setting.PersistConsumerOffsetInterval, Setting.PersistConsumerOffsetInterval));
            _logger.InfoFormat("[{0}] started.", Id);
            return this;
        }
        public Consumer Shutdown()
        {
            _remotingClient.Shutdown();
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }
            _logger.InfoFormat("[{0}] shutdown.", Id);
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
            foreach (var subscriptionTopic in _subscriptionTopics)
            {
                try
                {
                    RebalanceClustering(subscriptionTopic);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("[{0}]: rebalanceClustering for topic [{1}] has exception", Id, subscriptionTopic), ex);
                }
            }
        }
        private void RebalanceClustering(string subscriptionTopic)
        {
            List<string> consumerIdList;
            try
            {
                consumerIdList = QueryGroupConsumers(GroupName, subscriptionTopic).ToList();
                if (_consumerIds.Count != consumerIdList.Count)
                {
                    _logger.DebugFormat("[{0}]: consumerIds changed, old:{1}, new:{2}", Id, _consumerIds == null ? string.Empty : string.Join(",", _consumerIds), string.Join(",", consumerIdList));
                    _consumerIds = consumerIdList;
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: clustering rebalance failed as QueryGroupConsumers has exception.", Id), ex);
                return;
            }

            consumerIdList.Sort();

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
                    PersistOffset(pullRequest);
                    _logger.DebugFormat("[{0}]: removed pull request.[topic={1},queueId={2}]", Id, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId);
                }
            }

            // Check message queues to add.
            foreach (var messageQueue in messageQueues)
            {
                var key = messageQueue.ToString();
                PullRequest pullRequest;
                if (!_pullRequestDict.TryGetValue(key, out pullRequest))
                {
                    var queueOffset = -1L;
                    if (Setting.MessageModel == MessageModel.BroadCasting)
                    {
                        queueOffset = _localOffsetStore.GetQueueOffset(GroupName, messageQueue);
                    }
                    var request = new PullRequest(Id, GroupName, messageQueue, queueOffset, _remotingClient, Setting.MessageHandleMode, _messageHandler, Setting.PullRequestSetting);
                    if (_pullRequestDict.TryAdd(key, request))
                    {
                        request.Start();
                        _logger.DebugFormat("[{0}]: added pull request.[topic={1},queueId={2}]", Id, request.MessageQueue.Topic, request.MessageQueue.QueueId);
                    }
                }
            }
        }
        private void PersistOffset()
        {
            foreach (var pullRequest in _pullRequestDict.Values)
            {
                PersistOffset(pullRequest);
            }
        }
        private void PersistOffset(PullRequest pullRequest)
        {
            try
            {
                var consumedMinQueueOffset = pullRequest.ProcessQueue.GetConsumedMinQueueOffset();
                if (consumedMinQueueOffset >= 0)
                {
                    if (Setting.MessageModel == MessageModel.BroadCasting)
                    {
                        _localOffsetStore.PersistQueueOffset(GroupName, pullRequest.MessageQueue, consumedMinQueueOffset);
                    }
                    else if (Setting.MessageModel == MessageModel.Clustering)
                    {
                        var request = new UpdateQueueOffsetRequest(GroupName, pullRequest.MessageQueue, consumedMinQueueOffset);
                        var remotingRequest = new RemotingRequest((int)RequestCode.UpdateQueueOffsetRequest, _binarySerializer.Serialize(request));
                        _remotingClient.InvokeOneway(remotingRequest, 10000);
                        _logger.DebugFormat("Update consumed min queue offset to broker. Group:{0}, Topic:{1}, QueueId:{2}, Offset:{3}",
                            GroupName,
                            pullRequest.MessageQueue.Topic,
                            pullRequest.MessageQueue.QueueId,
                            consumedMinQueueOffset);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: PersistOffset has exception.", Id), ex);
            }
        }
        private void SendHeartbeat()
        {
            try
            {
                _remotingClient.InvokeOneway(new RemotingRequest(
                    (int)RequestCode.ConsumerHeartbeat,
                    _binarySerializer.Serialize(new ConsumerData(Id, GroupName, SubscriptionTopics))),
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
                    _logger.DebugFormat("[{0}]: topic queue count updated, topic:{1}, queueCount:{2}", Id, topic, topicQueueCountFromServer);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: UpdateTopicQueues failed, topic:{1}", Id, topic), ex);
            }
        }
        private IEnumerable<string> QueryGroupConsumers(string groupName, string topic)
        {
            var queryConsumerRequest = _binarySerializer.Serialize(new QueryConsumerRequest(groupName, topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryGroupConsumer, queryConsumerRequest);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
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
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
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
