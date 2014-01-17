using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicQueuesDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
        private readonly ConcurrentDictionary<MessageQueue, ProcessQueue> _processQueueDict = new ConcurrentDictionary<MessageQueue, ProcessQueue>();
        private readonly BlockingCollection<WrappedMessage> _messageQueue = new BlockingCollection<WrappedMessage>(new ConcurrentQueue<WrappedMessage>());
        private readonly List<string> _subscriptionTopics = new List<string>();
        private readonly BlockingCollection<PullRequest> _pullRequestBlockingQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());
        private readonly Worker _executePullReqeustWorker;
        private readonly Worker _handleMessageWorker;
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
            _handleMessageWorker = new Worker(HandleMessage);
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
            _remotingClient.Start();
            _handleMessageWorker.Start();
            _scheduleService.ScheduleTask(Rebalance, Settings.RebalanceInterval, Settings.RebalanceInterval);
            _scheduleService.ScheduleTask(UpdateAllLocalTopicQueues, Settings.UpdateTopicQueueCountInterval, Settings.UpdateTopicQueueCountInterval);
            _scheduleService.ScheduleTask(SendHeartbeatToBroker, Settings.HeartbeatBrokerInterval, Settings.HeartbeatBrokerInterval);
            _scheduleService.ScheduleTask(PersistOffset, Settings.PersistConsumerOffsetInterval, Settings.PersistConsumerOffsetInterval);
            _executePullReqeustWorker.Start();
            _logger.InfoFormat("[{0}] started, settings:{1}", Id, Settings);
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
                    _logger.WarnFormat("[{0}]: the consumer message buffer is full, so do flow control, [messageCount={1},pullRequest={2},flowControlTimes={3}]", Id, messageCount, pullRequest, flowControlTimes1);
                }
            }
            else if (messageSpan >= ConsumeMaxSpan)
            {
                EnqueuePullRequest(pullRequest, PullTimeDelayMillsWhenFlowControl);
                if ((flowControlTimes2++ % 3000) == 0)
                {
                    _logger.WarnFormat("[{0}]: the consumer message span too long, so do flow control, [messageSpan={1},pullRequest={2},flowControlTimes={3}]", Id, messageSpan, pullRequest, flowControlTimes2);
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
                if (AllowExecutePullRequest(pullRequest))
                {
                    PullMessage(pullRequest);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: executePullRequest exception. PullRequest: {1}.", Id, pullRequest), ex);
            }
        }
        private bool AllowExecutePullRequest(PullRequest pullRequest)
        {
            return _processQueueDict.Keys.Where(x => x.Topic == pullRequest.MessageQueue.Topic).Any(x => x.QueueId == pullRequest.MessageQueue.QueueId);
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
                pullResult.Messages.ForEach(x => _messageQueue.Add(new WrappedMessage(x, pullRequest.MessageQueue, pullRequest.ProcessQueue)));
            }
            EnqueuePullRequest(pullRequest);
        }
        private void HandleMessage()
        {
            var wrappedMessage = _messageQueue.Take();
            Action handleAction = () =>
            {
                try
                {
                    _messageHandler.Handle(wrappedMessage.QueueMessage);
                }
                catch { }  //TODO,处理失败的消息放到本地队列继续重试消费
                var offset = wrappedMessage.ProcessQueue.RemoveMessage(wrappedMessage.QueueMessage);
                if (offset >= 0)
                {
                    _offsetStore.UpdateOffset(wrappedMessage.MessageQueue, offset);
                }
            };
            if (Settings.MessageHandleMode == MessageHandleMode.Sequential)
            {
                handleAction();
            }
            else if (Settings.MessageHandleMode == MessageHandleMode.Parallel)
            {
                Task.Factory.StartNew(handleAction);
            }
        }
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
                UpdateProcessQueueDict(subscriptionTopic, messageQueues);
            }
        }
        private void RebalanceClustering(string subscriptionTopic, IList<string> consumerIdList)
        {
            IList<MessageQueue> messageQueues;
            if (_topicQueuesDict.TryGetValue(subscriptionTopic, out messageQueues))
            {
                var messageQueueList = messageQueues.ToList();
                messageQueueList.Sort(new Comparison<MessageQueue>((x, y) =>
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
                }));

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

                UpdateProcessQueueDict(subscriptionTopic, allocatedMessageQueues.ToList());
            }
        }
        private IEnumerable<string> QueryGroupConsumers(string groupName)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryGroupConsumer, Encoding.UTF8.GetBytes(groupName));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var consumerIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return consumerIds.Split(new [] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("[{0}]: queryGroupConsumers has exception, remoting response code:{1}", Id, remotingResponse.Code));
            }
        }
        private void UpdateProcessQueueDict(string topic, IList<MessageQueue> messageQueues)
        {
            // Check message queues to remove
            var toRemoveMessageQueues = new List<MessageQueue>();
            foreach (var messageQueue in _processQueueDict.Keys)
            {
                if (messageQueue.Topic == topic)
                {
                    if (!messageQueues.Any(x => x.Topic == messageQueue.Topic && x.QueueId == messageQueue.QueueId))
                    {
                        toRemoveMessageQueues.Add(messageQueue);
                    }
                }
            }
            foreach (var messageQueue in toRemoveMessageQueues)
            {
                ProcessQueue processQueue;
                if (_processQueueDict.TryRemove(messageQueue, out processQueue))
                {
                    PersistRemovedMessageQueueOffset(messageQueue);
                    _logger.InfoFormat("[{0}]: removed message queue:{1}, consumerGroup:{2}", Id, messageQueue, GroupName);
                }
            }

            // Check message queues to add.
            var pullRequestList = new List<PullRequest>();
            foreach (var messageQueue in messageQueues)
            {
                if (!_processQueueDict.Any(x => x.Key.Topic == messageQueue.Topic && x.Key.QueueId == messageQueue.QueueId))
                {
                    var pullRequest = new PullRequest();
                    pullRequest.ConsumerGroup = GroupName;
                    pullRequest.MessageQueue = messageQueue;
                    pullRequest.ProcessQueue = new ProcessQueue();

                    long nextOffset = ComputePullFromWhere(messageQueue);
                    if (nextOffset >= 0)
                    {
                        pullRequest.NextOffset = nextOffset;
                        if (_processQueueDict.TryAdd(messageQueue, pullRequest.ProcessQueue))
                        {
                            pullRequestList.Add(pullRequest);
                        }
                    }
                    else
                    {
                        _logger.WarnFormat("[{0}]: the new messageQueue:{1} (consumerGroup:{2}) cannot be added as the nextOffset is < 0.", Id, messageQueue, GroupName);
                    }
                }
            }

            foreach (var pullRequest in pullRequestList)
            {
                var nextOffset = pullRequest.NextOffset;
                EnqueuePullRequest(pullRequest);
                _logger.InfoFormat("[{0}]: added message queue:{1}, consumerGroup:{2}, nextOffset:{3}", Id, pullRequest.MessageQueue, GroupName, nextOffset);
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
            try
            {
                _remotingClient.InvokeOneway(new RemotingRequest(
                    (int)RequestCode.ConsumerHeartbeat,
                    _binarySerializer.Serialize(new ConsumerData(Id, GroupName, MessageModel, SubscriptionTopics))),
                    3000);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("[{0}]: send heart beat to broker exception", Id), ex);
            }
        }
        private void UpdateAllLocalTopicQueues()
        {
            foreach (var topic in SubscriptionTopics)
            {
                UpdateLocalTopicQueues(topic);
            }
        }
        private void UpdateLocalTopicQueues(string topic)
        {
            try
            {
                var topicQueueCountFromServer = GetTopicQueueCountFromServer(topic);
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
        private int GetTopicQueueCountFromServer(string topic)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.GetTopicQueueCount, Encoding.UTF8.GetBytes(topic));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, 10000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                return BitConverter.ToInt32(remotingResponse.Body, 0);
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueCountFromServer has exception, remoting response code:{0}", remotingResponse.Code));
            }
        }

        #endregion
    }
}
