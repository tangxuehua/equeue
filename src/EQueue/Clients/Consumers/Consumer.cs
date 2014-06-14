using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private readonly object _lockObject;
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly List<string> _subscriptionTopics;
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicQueuesDict;
        private readonly ConcurrentDictionary<string, PullRequest> _pullRequestDict;
        private readonly List<int> _taskIds;
        private readonly IScheduleService _scheduleService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly ILogger _logger;
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

        public Consumer(string id, string groupName) : this(id, groupName, new ConsumerSetting()) { }
        public Consumer(string id, string groupName, ConsumerSetting setting)
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }
            if (groupName == null)
            {
                throw new ArgumentNullException("groupName");
            }
            Id = id;
            GroupName = groupName;
            Setting = setting ?? new ConsumerSetting();

            _lockObject = new object();
            _subscriptionTopics = new List<string>();
            _topicQueuesDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
            _pullRequestDict = new ConcurrentDictionary<string, PullRequest>();
            _taskIds = new List<int>();
            _remotingClient = new SocketRemotingClient(Setting.BrokerAddress, Setting.BrokerPort);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        #endregion

        #region Public Methods

        public Consumer SetMessageHandler(IMessageHandler messageHandler)
        {
            if (messageHandler == null)
            {
                throw new ArgumentNullException("messageHandler");
            }
            _messageHandler = messageHandler;
            return this;
        }
        public Consumer Start()
        {
            _remotingClient.Connect();
            _remotingClient.Start();
            StartBackgroundJobs();
            _remotingClient.ClientSocketConnectionChanged += HandleRemotingClientConnectionChanged;
            _logger.InfoFormat("Started, consumerId:{0}, group:{1}.", Id, GroupName);
            return this;
        }
        public Consumer Shutdown()
        {
            _remotingClient.Shutdown();
            _remotingClient.ClientSocketConnectionChanged -= HandleRemotingClientConnectionChanged;
            StopBackgroundJobs();
            _logger.InfoFormat("Shutdown, consumerId:{0}, group:{1}.", Id, GroupName);
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
                    _logger.Error(string.Format("RebalanceClustering has exception, consumerId:{0}, group:{1}, topic:{2}", Id, GroupName, subscriptionTopic), ex);
                }
            }
        }
        private void RebalanceClustering(string subscriptionTopic)
        {
            List<string> consumerIdList;
            try
            {
                consumerIdList = QueryGroupConsumers(subscriptionTopic).ToList();
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("RebalanceClustering failed as QueryGroupConsumers has exception, consumerId:{0}, group:{1}, topic:{2}", Id, GroupName, subscriptionTopic), ex);
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
                    _logger.Error(string.Format("Allocate message queue has exception, consumerId:{0}, group:{1}, topic:{2}", Id, GroupName, subscriptionTopic), ex);
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
                    _logger.DebugFormat("Removed pull request, consumerId:{0}, group:{1}, topic={2}, queueId={3}", Id, GroupName, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId);
                }
            }

            // Check message queues to add.
            foreach (var messageQueue in messageQueues)
            {
                var key = messageQueue.ToString();
                PullRequest pullRequest;
                if (!_pullRequestDict.TryGetValue(key, out pullRequest))
                {
                    var request = new PullRequest(Id, GroupName, messageQueue, -1, _remotingClient, Setting.MessageHandleMode, _messageHandler, Setting.PullRequestSetting);
                    if (_pullRequestDict.TryAdd(key, request))
                    {
                        request.Start();
                        _logger.DebugFormat("Added pull request, consumerId:{0}, group:{1}, topic={2}, queueId={3}", Id, GroupName, request.MessageQueue.Topic, request.MessageQueue.QueueId);
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
                    if (!pullRequest.ProcessQueue.TryUpdatePreviousConsumedMinQueueOffset(consumedMinQueueOffset))
                    {
                        return;
                    }

                    var request = new UpdateQueueOffsetRequest(GroupName, pullRequest.MessageQueue, consumedMinQueueOffset);
                    var remotingRequest = new RemotingRequest((int)RequestCode.UpdateQueueOffsetRequest, _binarySerializer.Serialize(request));
                    _remotingClient.InvokeOneway(remotingRequest, 10000);
                    _logger.DebugFormat("Sent queue consume offset to broker. group:{0}, consumerId:{1}, topic:{2}, queueId:{3}, offset:{4}",
                        GroupName,
                        Id,
                        pullRequest.MessageQueue.Topic,
                        pullRequest.MessageQueue.QueueId,
                        consumedMinQueueOffset);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("PersistOffset has exception, consumerId:{0}, group:{1}, topic:{2}, queueId:{3}", Id, GroupName, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId), ex);
            }
        }
        private void SendHeartbeat()
        {
            try
            {
                _remotingClient.InvokeOneway(new RemotingRequest(
                    (int)RequestCode.ConsumerHeartbeat,
                    _binarySerializer.Serialize(new ConsumerData(Id, GroupName, _subscriptionTopics))),
                    3000);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("SendHeartbeat remoting request to broker has exception, consumerId:{0}, group:{1}", Id, GroupName), ex);
            }
        }
        private void RefreshTopicQueues()
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
                    _logger.DebugFormat("Queue count of topic updated, consumerId:{0}, group:{1}, topic:{2}, queueCount:{3}", Id, GroupName, topic, topicQueueCountFromServer);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("UpdateTopicQueues failed, consumerId:{0}, group:{1}, topic:{2}", Id, GroupName, topic), ex);
            }
        }
        private IEnumerable<string> QueryGroupConsumers(string topic)
        {
            var queryConsumerRequest = _binarySerializer.Serialize(new QueryConsumerRequest(GroupName, topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryGroupConsumer, queryConsumerRequest);
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 30000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var consumerIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return consumerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("QueryGroupConsumers has exception, consumerId:{0}, group:{1}, topic:{2}, remoting response code:{3}", Id, GroupName, topic, remotingResponse.Code));
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
                throw new Exception(string.Format("GetTopicQueueCount has exception, consumerId:{0}, group:{1}, topic:{2}, remoting response code:{3}", Id, GroupName, topic, remotingResponse.Code));
            }
        }
        private void HandleRemotingClientConnectionChanged(bool isConnected)
        {
            if (isConnected)
            {
                StartBackgroundJobs();
            }
            else
            {
                StopBackgroundJobs();
            }
        }
        private void StartBackgroundJobs()
        {
            lock (_lockObject)
            {
                StopBackgroundJobsInternal();
                StartBackgroundJobsInternal();
            }
        }
        private void StopBackgroundJobs()
        {
            lock (_lockObject)
            {
                StopBackgroundJobsInternal();
            }
        }
        private void StartBackgroundJobsInternal()
        {
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.RefreshTopicQueues", RefreshTopicQueues, Setting.UpdateTopicQueueCountInterval, Setting.UpdateTopicQueueCountInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.SendHeartbeat", SendHeartbeat, Setting.HeartbeatBrokerInterval, Setting.HeartbeatBrokerInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.Rebalance", Rebalance, Setting.RebalanceInterval, Setting.RebalanceInterval));
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.PersistOffset", PersistOffset, Setting.PersistConsumerOffsetInterval, Setting.PersistConsumerOffsetInterval));
            _logger.DebugFormat("Background job started, consumerId:{0}, group:{1}", Id, GroupName);
        }
        private void StopBackgroundJobsInternal()
        {
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }
            foreach (var pullRequest in _pullRequestDict.Values)
            {
                pullRequest.Stop();
            }
            Clear();
            _logger.DebugFormat("Background job stop requesting sent, consumerId:{0}, group:{1}", Id, GroupName);
        }
        private void Clear()
        {
            _taskIds.Clear();
            _pullRequestDict.Clear();
            _topicQueuesDict.Clear();
        }

        #endregion
    }
}
