using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EQueue.Infrastructure;
using EQueue.Infrastructure.Extensions;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Infrastructure.Scheduling;
using EQueue.Protocols;
using EQueue.Remoting;
using EQueue.Remoting.Requests;
using EQueue.Remoting.Responses;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private const int PullRequestTimeoutMilliseconds = 30 * 1000;
        private long flowControlTimes1;
        private long flowControlTimes2;
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IDictionary<string, TopicRouteData> _topicRouteDataDict = new Dictionary<string, TopicRouteData>();
        private readonly ConcurrentDictionary<MessageQueue, ProcessQueue> _processQueueDict = new ConcurrentDictionary<MessageQueue, ProcessQueue>();
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicSubscribeInfoDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
        private readonly List<string> _subscriptionTopics = new List<string>();
        private readonly BlockingCollection<PullRequest> _pullRequestBlockingQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());
        private readonly Worker _executePullReqeustWorker;
        private readonly IScheduleService _scheduleService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly IOffsetStore _offsetStore;
        private readonly IMessageHandler _messageHandler;
        private readonly ILogger _logger;

        #endregion

        #region Public Properties

        public string Id { get; private set; }
        public ConsumerSettings Settings { get; private set; }
        public string GroupName { get; private set; }
        public MessageModel MessageModel { get; private set; }
        public IEnumerable<string> SubscriptionTopics
        {
            get { return _subscriptionTopics; }
        }
        public int PullThresholdForQueue { get; set; }
        public int ConsumeMaxSpan { get; set; }
        public int PullTimeDelayMillsWhenFlowControl { get; set; }
        public int PullMessageBatchSize { get; set; }

        #endregion

        #region Constructors

        public Consumer(ConsumerSettings settings, string groupName, MessageModel messageModel, IMessageHandler messageHandler)
            : this(string.Format("Consumer@{0}", Utils.GetLocalIPV4()), settings, groupName, messageModel, messageHandler)
        {
        }
        public Consumer(string id, ConsumerSettings settings, string groupName, MessageModel messageModel, IMessageHandler messageHandler)
        {
            Id = id;
            Settings = settings;
            GroupName = groupName;
            MessageModel = messageModel;

            _messageHandler = messageHandler;
            _remotingClient = new SocketRemotingClient(settings.BrokerAddress, settings.BrokerPort);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _offsetStore = ObjectContainer.Resolve<IOffsetStore>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _executePullReqeustWorker = new Worker(ExecutePullRequest);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);

            PullThresholdForQueue = 1000;
            ConsumeMaxSpan = 2000;
            PullTimeDelayMillsWhenFlowControl = 100;
            PullMessageBatchSize = 32;
        }

        #endregion

        #region Public Methods

        public Consumer Start()
        {
            _scheduleService.ScheduleTask(Rebalance, 1000 * 10, 1000 * 10);
            _scheduleService.ScheduleTask(UpdateAllTopicRouteData, 1000 * 30, 1000 * 30);
            _scheduleService.ScheduleTask(SendHeartbeatToBroker, 1000 * 30, 1000 * 30);
            _scheduleService.ScheduleTask(PersistOffset, 1000 * 5, 1000 * 5);
            _executePullReqeustWorker.Start();
            _logger.InfoFormat("Consumer [{0}] start OK, settings:{1}", Id, Settings);
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

        #endregion

        #region Private Methods

        private void PullMessage(PullRequest pullRequest)
        {
            var messageCount = pullRequest.ProcessQueue.GetMessageCount();
            var messageSpan = pullRequest.ProcessQueue.GetMessageSpan();

            if (messageCount >= PullThresholdForQueue)
            {
                EnqueuePullRequest(pullRequest, PullTimeDelayMillsWhenFlowControl);
                if ((flowControlTimes1++ % 3000) == 0)
                {
                    _logger.WarnFormat("The consumer message buffer is full, so do flow control, [messageCount={0},pullRequest={1},flowControlTimes={2}]", messageCount, pullRequest, flowControlTimes1);
                }
            }
            else if (messageSpan >= ConsumeMaxSpan)
            {
                EnqueuePullRequest(pullRequest, PullTimeDelayMillsWhenFlowControl);
                if ((flowControlTimes2++ % 3000) == 0)
                {
                    _logger.WarnFormat("The consumer message span too long, so do flow control, [messageSpan={0},pullRequest={1},flowControlTimes={2}]", messageSpan, pullRequest, flowControlTimes2);
                }
            }
            else
            {
                StartPullMessageTask(pullRequest).ContinueWith((task) => ProcessPullResult(pullRequest, task.Result));
            }
        }
        private void EnqueuePullRequest(PullRequest pullRequest)
        {
            _pullRequestBlockingQueue.Add(pullRequest);
        }
        private void EnqueuePullRequest(PullRequest pullRequest, int millisecondsDelay)
        {
            Task.Factory.StartDelayedTask(millisecondsDelay, () => _pullRequestBlockingQueue.Add(pullRequest));
        }
        private void ExecutePullRequest()
        {
            var pullRequest = _pullRequestBlockingQueue.Take();
            try
            {
                PullMessage(pullRequest);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("ExecutePullRequest exception. PullRequest: {0}.", pullRequest), ex);
            }
        }
        private Task<PullResult> StartPullMessageTask(PullRequest pullRequest)
        {
            var request = new PullMessageRequest
            {
                ConsumerGroup = GroupName,
                MessageQueue = pullRequest.MessageQueue,
                QueueOffset = pullRequest.NextOffset,
                PullMessageBatchSize = PullMessageBatchSize
            };
            var data = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)RequestCode.PullMessage, data);
            var taskCompletionSource = new TaskCompletionSource<PullResult>();
            _remotingClient.InvokeAsync(remotingRequest, PullRequestTimeoutMilliseconds).ContinueWith((requestTask) =>
            {
                var remotingResponse = requestTask.Result;
                if (remotingResponse != null)
                {
                    var response = _binarySerializer.Deserialize<PullMessageResponse>(remotingResponse.Body);
                    var result = new PullResult
                    {
                        PullStatus = (PullStatus)remotingResponse.Code,
                        Messages = response.Messages
                    };
                    taskCompletionSource.SetResult(result);
                }
                else
                {
                    taskCompletionSource.SetResult(new PullResult { PullStatus = PullStatus.Failed });
                }
            });
            return taskCompletionSource.Task;
        }
        private void ProcessPullResult(PullRequest pullRequest, PullResult pullResult)
        {
            if (pullResult.PullStatus == PullStatus.Found && pullResult.Messages.Count() > 0)
            {
                pullRequest.NextOffset += pullResult.Messages.Count();
                pullRequest.ProcessQueue.AddMessages(pullResult.Messages);
                StartConsumeTask(pullRequest, pullResult);
            }
            EnqueuePullRequest(pullRequest);
        }
        private void StartConsumeTask(PullRequest pullRequest, PullResult pullResult)
        {
            Task.Factory.StartNew(() =>
            {
                foreach (var message in pullResult.Messages)
                {
                    try
                    {
                        _messageHandler.Handle(message);
                    }
                    catch { }  //TODO,处理失败的消息放到本地队列继续重试消费
                }
                long offset = pullRequest.ProcessQueue.RemoveMessages(pullResult.Messages);
                if (offset >= 0)
                {
                    _offsetStore.UpdateOffset(pullRequest.MessageQueue, offset);
                }
            });
        }
        private void RebalanceBroadCasting(string topic)
        {
            if (_topicSubscribeInfoDict.ContainsKey(topic))
            {
                var messageQueues = _topicSubscribeInfoDict[topic];
                var changed = UpdateProcessQueueDict(topic, messageQueues);
                if (changed)
                {
                    _logger.InfoFormat("messageQueueChanged [consumerGroup:{0}, topic:{1}, allocatedMessageQueues:{2}]", GroupName, topic, string.Join("|", messageQueues));
                }
            }
            else
            {
                _logger.WarnFormat("DoRebalance of broad casting, consumerGroup: {0}, but the topic[{1}] not exist.", GroupName, topic);
            }
        }
        private void RebalanceClustering(string topic)
        {
            if (_topicSubscribeInfoDict.ContainsKey(topic))
            {
                var messageQueues = _topicSubscribeInfoDict[topic];
                var consumerIds = FindConsumers(GroupName);
                var messageQueueList = messageQueues.ToList();
                var consumerClientIdList = consumerIds.ToList();
                messageQueueList.Sort();
                consumerClientIdList.Sort();

                IEnumerable<MessageQueue> allocatedMessageQueues = new List<MessageQueue>();
                try
                {
                    allocatedMessageQueues = _allocateMessageQueueStragegy.Allocate(Id, messageQueueList, consumerClientIdList);
                }
                catch (Exception ex)
                {
                    _logger.Error("Allocate message queue has exception.", ex);
                }

                var allocatedMessageQueueList = allocatedMessageQueues.ToList();
                var changed = UpdateProcessQueueDict(topic, allocatedMessageQueueList);
                if (changed)
                {
                    _logger.InfoFormat("messageQueueChanged [consumerGroup:{0}, topic:{1}, allocatedMessageQueues:{2}, consumerClientIds:{3}]", GroupName, topic, string.Join("|", allocatedMessageQueueList), string.Join("|", consumerClientIdList));
                }
            }
            else
            {
                _logger.WarnFormat("DoRebalance of clustering, consumerGroup: {0}, but the topic[{1}] not exist.", GroupName, topic);
            }
        }
        private IEnumerable<string> FindConsumers(string consumerGroup)
        {
            //TODO
            return new string[0];
        }
        private bool UpdateProcessQueueDict(string topic, IList<MessageQueue> messageQueues)
        {
            var changed = false;

            foreach (var messageQueue in _processQueueDict.Keys)
            {
                if (messageQueue.Topic == topic)
                {
                    if (!messageQueues.Contains(messageQueue))
                    {
                        changed = true;
                        ProcessQueue processQueue;
                        _processQueueDict.TryRemove(messageQueue, out processQueue);
                        PersistRemovedMessageQueueOffset(messageQueue);
                    }
                }
            }

            var pullRequestList = new List<PullRequest>();
            foreach (var messageQueue in messageQueues)
            {
                if (!_processQueueDict.ContainsKey(messageQueue))
                {
                    var pullRequest = new PullRequest();
                    pullRequest.ConsumerGroup = GroupName;
                    pullRequest.MessageQueue = messageQueue;
                    pullRequest.ProcessQueue = new ProcessQueue();

                    long nextOffset = ComputePullFromWhere(messageQueue);
                    if (nextOffset >= 0)
                    {
                        changed = true;
                        pullRequest.NextOffset = nextOffset;
                        pullRequestList.Add(pullRequest);
                        _processQueueDict.TryAdd(messageQueue, pullRequest.ProcessQueue);
                        _logger.InfoFormat("DoRebalance, ConsumerGroup: {0}, Add a new messageQueue, {1}", GroupName, messageQueue);
                    }
                    else
                    {
                        _logger.WarnFormat("DoRebalance, ConsumerGroup: {0}, The new messageQueue {1} cannot be added as the nextOffset is < 0.", GroupName, messageQueue);
                    }
                }
            }

            DispatchPullRequest(pullRequestList);

            return changed;
        }
        private void TruncateMessageQueueNotMyTopic()
        {
            var shouldRemoveQueues = new List<MessageQueue>();
            foreach (var messageQueue in _processQueueDict.Keys)
            {
                if (!_subscriptionTopics.Contains(messageQueue.Topic))
                {
                    shouldRemoveQueues.Add(messageQueue);
                }
            }
            foreach (var queue in shouldRemoveQueues)
            {
                ProcessQueue removedQueue;
                _processQueueDict.TryRemove(queue, out removedQueue);
            }
        }
        private void PersistRemovedMessageQueueOffset(MessageQueue messageQueue)
        {
            _offsetStore.Persist(messageQueue);
            _offsetStore.RemoveOffset(messageQueue);
        }
        private void DispatchPullRequest(IEnumerable<PullRequest> pullRequestList)
        {
            foreach (var pullRequest in pullRequestList)
            {
                EnqueuePullRequest(pullRequest);
                _logger.InfoFormat("doRebalance, consumerGroup:{0}, add a new pull request {1}", GroupName, pullRequest);
            }
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
        private void Rebalance()
        {
            if (MessageModel == MessageModel.BroadCasting)
            {
                foreach (var topic in _subscriptionTopics)
                {
                    try
                    {
                        RebalanceBroadCasting(topic);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("RebalanceBroadCasting has exception", ex);
                    }
                }
            }
            else if (MessageModel == MessageModel.Clustering)
            {
                foreach (var topic in _subscriptionTopics)
                {
                    try
                    {
                        RebalanceClustering(topic);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error("RebalanceClustering has exception", ex);
                    }
                }
            }

            TruncateMessageQueueNotMyTopic();
        }
        private void PersistOffset()
        {
            foreach (var messageQueue in _processQueueDict.Keys)
            {
                try
                {
                    _offsetStore.Persist(messageQueue);
                }
                catch (Exception ex)
                {
                    _logger.Error("PersistOffset exception.", ex);
                }
            }
        }
        private void SendHeartbeatToBroker()
        {
            var heartbeatData = new HeartbeatData(Id, new ConsumerData(GroupName, MessageModel, SubscriptionTopics));

            try
            {
                //TODO
                //this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                _logger.InfoFormat("Send heart beat to broker[{0}] success, heartbeatData:[{1}]", Settings.BrokerAddress, heartbeatData);
            }
            catch (Exception ex)
            {
                _logger.Error("Send heart beat to broker exception", ex);
            }
        }

        #region Update topic route data

        private void UpdateAllTopicRouteData()
        {
            foreach (var topic in SubscriptionTopics)
            {
                UpdateTopicRouteData(topic);
            }
        }
        private void UpdateTopicRouteData(string topic)
        {
            var topicRouteDataFromServer = GetTopicRouteDataFromServer(topic);
            var topicRouteDataOnLocal = _topicRouteDataDict[topic];
            var changed = IsTopicRouteDataChanged(topicRouteDataOnLocal, topicRouteDataFromServer);

            if (!changed)
            {
                changed = IsNeedUpdateTopicRouteData(topic);
            }

            if (changed)
            {
                var consumeMessageQueues = new List<MessageQueue>();
                for (var index = 0; index < topicRouteDataFromServer.ConsumeQueueCount; index++)
                {
                    consumeMessageQueues.Add(new MessageQueue(topic, index));
                }

                _topicSubscribeInfoDict[topic] = consumeMessageQueues.ToList();
                _topicRouteDataDict[topic] = topicRouteDataFromServer;
            }
        }
        private TopicRouteData GetTopicRouteDataFromServer(string topic)
        {
            //TODO
            return null;
        }
        private bool IsTopicRouteDataChanged(TopicRouteData oldData, TopicRouteData newData)
        {
            return oldData != newData;
        }
        private bool IsNeedUpdateTopicRouteData(string topic)
        {
            if (!_subscriptionTopics.Any(x => x == topic))
            {
                return true;
            }
            return false;
        }

        #endregion

        #endregion
    }
}
