using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Remoting.Exceptions;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Socketing;
using ECommon.TcpTransport;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class Consumer : ISocketClientEventListener
    {
        #region Private Members

        private readonly object _lockObject;
        private readonly SocketRemotingClient _remotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly List<string> _subscriptionTopics;
        private readonly List<int> _taskIds;
        private readonly TaskFactory _taskFactory;
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicQueuesDict;
        private readonly ConcurrentDictionary<string, PullRequest> _pullRequestDict;
        private readonly ConcurrentDictionary<long, ConsumingMessage> _handlingMessageDict;
        private readonly BlockingCollection<PullRequest> _pullRequestQueue;
        private readonly BlockingCollection<ConsumingMessage> _consumingMessageQueue;
        private readonly BlockingCollection<ConsumingMessage> _messageRetryQueue;
        private readonly IScheduleService _scheduleService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly Worker _executePullRequestWorker;
        private readonly Worker _handleMessageWorker;
        private readonly ILogger _logger;
        private readonly AutoResetEvent _waitSocketConnectHandle;
        private IMessageHandler _messageHandler;
        private long _flowControlTimes;
        private bool _stoped;
        private bool _isBrokerServerConnected;

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
            _pullRequestQueue = new BlockingCollection<PullRequest>(new ConcurrentQueue<PullRequest>());
            _pullRequestDict = new ConcurrentDictionary<string, PullRequest>();
            _consumingMessageQueue = new BlockingCollection<ConsumingMessage>(new ConcurrentQueue<ConsumingMessage>());
            _messageRetryQueue = new BlockingCollection<ConsumingMessage>(new ConcurrentQueue<ConsumingMessage>());
            _handlingMessageDict = new ConcurrentDictionary<long, ConsumingMessage>();
            _taskIds = new List<int>();
            _taskFactory = new TaskFactory(new LimitedConcurrencyLevelTaskScheduler(Setting.ConsumeThreadMaxCount));
            _remotingClient = new SocketRemotingClient(Setting.BrokerConsumerIPEndPoint, null, this);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _executePullRequestWorker = new Worker("Consumer.ExecutePullRequest", ExecutePullRequest);
            _handleMessageWorker = new Worker("Consumer.HandleMessage", HandleMessage);
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _waitSocketConnectHandle = new AutoResetEvent(false);
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
            _stoped = false;
            _remotingClient.Start();
            _waitSocketConnectHandle.WaitOne();
            _logger.InfoFormat("Started, consumerId:{0}, group:{1}.", Id, GroupName);
            return this;
        }
        public Consumer Shutdown()
        {
            _stoped = true;
            _remotingClient.Shutdown();
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

        private void ExecutePullRequest()
        {
            var pullRequest = _pullRequestQueue.Take();

            if (_stoped) return;

            if (pullRequest != null)
            {
                PullMessage(pullRequest);
            }
        }
        private void PullMessage(PullRequest pullRequest)
        {
            try
            {
                if (_stoped) return;
                if (pullRequest.ProcessQueue.IsDropped) return;

                var messageCount = pullRequest.ProcessQueue.GetMessageCount();

                if (messageCount >= Setting.PullThresholdForQueue)
                {
                    Task.Factory.StartDelayedTask(Setting.PullTimeDelayMillsWhenFlowControl, () => SchedulePullRequest(pullRequest));
                    if ((_flowControlTimes++ % 100) == 0)
                    {
                        _logger.WarnFormat("Detect that the message process queue has too many messages, so do flow control. pullRequest={0}, queueMessageCount={1}, flowControlTimes={2}", pullRequest, messageCount, _flowControlTimes);
                    }
                    return;
                }

                var request = new PullMessageRequest
                {
                    ConsumerGroup = GroupName,
                    MessageQueue = pullRequest.MessageQueue,
                    QueueOffset = pullRequest.NextConsumeOffset,
                    PullMessageBatchSize = Setting.PullMessageBatchSize,
                    SuspendPullRequestMilliseconds = Setting.SuspendPullRequestMilliseconds,
                    ConsumeFromWhere = Setting.ConsumeFromWhere
                };
                var data = SerializePullMessageRequest(request);
                var remotingRequest = new RemotingRequest((int)RequestCode.PullMessage, data);

                pullRequest.PullStartTime = DateTime.Now;
                _remotingClient.InvokeAsync(remotingRequest, Setting.PullRequestTimeoutMilliseconds).ContinueWith(pullTask =>
                {
                    try
                    {
                        if (_stoped) return;
                        if (pullRequest.ProcessQueue.IsDropped) return;

                        if (pullTask.Exception != null)
                        {
                            _logger.Error(string.Format("Pull message failed, pullRequest:{0}", pullRequest), pullTask.Exception);
                            SchedulePullRequest(pullRequest);
                            return;
                        }

                        ProcessPullResponse(pullRequest, pullTask.Result);
                    }
                    catch (Exception ex)
                    {
                        if (_stoped) return;
                        if (pullRequest.ProcessQueue.IsDropped) return;
                        if (_isBrokerServerConnected)
                        {
                            string remotingResponseBodyLength;
                            if (pullTask.Result != null)
                            {
                                remotingResponseBodyLength = pullTask.Result.Body.Length.ToString();
                            }
                            else
                            {
                                remotingResponseBodyLength = "pull message result is null.";
                            }
                            _logger.Error(string.Format("Process pull result has exception, pullRequest:{0}, remotingResponseBodyLength:{1}", pullRequest, remotingResponseBodyLength), ex);
                        }
                        SchedulePullRequest(pullRequest);
                    }
                });
            }
            catch (Exception ex)
            {
                if (_stoped) return;
                if (pullRequest.ProcessQueue.IsDropped) return;

                if (_isBrokerServerConnected)
                {
                    _logger.Error(string.Format("PullMessage has exception, pullRequest:{0}", pullRequest), ex);
                }
                SchedulePullRequest(pullRequest);
            }
        }
        private void ProcessPullResponse(PullRequest pullRequest, RemotingResponse remotingResponse)
        {
            if (remotingResponse == null)
            {
                _logger.ErrorFormat("Pull message response is null, pullRequest:{0}", pullRequest);
                SchedulePullRequest(pullRequest);
                return;
            }

            if (remotingResponse.Code == -1)
            {
                _logger.ErrorFormat("Pull message failed, pullRequest:{0}, errorMsg:{1}", pullRequest, Encoding.UTF8.GetString(remotingResponse.Body));
                SchedulePullRequest(pullRequest);
                return;
            }

            if (remotingResponse.Code == (int)PullStatus.Found)
            {
                var messages = _binarySerializer.Deserialize<IEnumerable<QueueMessage>>(remotingResponse.Body);
                if (messages.Count() > 0)
                {
                    pullRequest.ProcessQueue.AddMessages(messages);
                    foreach (var message in messages)
                    {
                        _consumingMessageQueue.Add(new ConsumingMessage(message, pullRequest.ProcessQueue));
                    }
                    pullRequest.NextConsumeOffset = messages.Last().QueueOffset + 1;
                }
            }
            else if (remotingResponse.Code == (int)PullStatus.NextOffsetReset)
            {
                var newOffset = BitConverter.ToInt64(remotingResponse.Body, 0);
                var oldOffset = pullRequest.NextConsumeOffset;
                pullRequest.NextConsumeOffset = newOffset;
                _logger.InfoFormat("Reset queue next consume offset. topic:{0}, queueId:{1}, old offset:{2}, new offset:{3}", pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId, oldOffset, newOffset);
            }
            else if (remotingResponse.Code == (int)PullStatus.NoNewMessage)
            {
                _logger.DebugFormat("No new message found, pullRequest:{0}", pullRequest);
            }
            else if (remotingResponse.Code == (int)PullStatus.Ignored)
            {
                _logger.InfoFormat("Pull request was ignored, pullRequest:{0}", pullRequest);
                return;
            }

            //Schedule the next pull request.
            SchedulePullRequest(pullRequest);
        }
        private void SchedulePullRequest(PullRequest pullRequest)
        {
            _pullRequestQueue.Add(pullRequest);
        }
        private void HandleMessage()
        {
            var consumingMessage = _consumingMessageQueue.Take();

            if (_stoped) return;
            if (consumingMessage == null) return;
            if (consumingMessage.ProcessQueue.IsDropped) return;

            var handleAction = new Action(() =>
            {
                if (!_handlingMessageDict.TryAdd(consumingMessage.Message.MessageOffset, consumingMessage))
                {
                    _logger.WarnFormat("Ignore to handle message [messageOffset={0}, topic={1}, queueId={2}, queueOffset={3}, consumerId={4}, group={5}], as it is being handling.",
                        consumingMessage.Message.MessageOffset,
                        consumingMessage.Message.Topic,
                        consumingMessage.Message.QueueId,
                        consumingMessage.Message.QueueOffset,
                        Id,
                        GroupName);
                    return;
                }
                HandleMessage(consumingMessage);
            });

            if (Setting.MessageHandleMode == MessageHandleMode.Sequential)
            {
                handleAction();
            }
            else if (Setting.MessageHandleMode == MessageHandleMode.Parallel)
            {
                _taskFactory.StartNew(handleAction);
            }
        }
        private void RetryMessage()
        {
            HandleMessage(_messageRetryQueue.Take());
        }
        private void HandleMessage(ConsumingMessage consumingMessage)
        {
            if (_stoped) return;
            if (consumingMessage == null) return;
            if (consumingMessage.ProcessQueue.IsDropped) return;

            try
            {
                _messageHandler.Handle(consumingMessage.Message, new MessageContext(currentQueueMessage => RemoveHandledMessage(consumingMessage)));
            }
            catch (Exception ex)
            {
                //TODO，目前，对于消费失败（遇到异常）的消息，我们先记录错误日志，然后将该消息放入本地内存的重试队列；
                //放入重试队列后，会定期对该消息进行重试，重试队列中的消息会每隔1s被取出一个来重试。
                //通过这样的设计，可以确保消费有异常的消息不会被认为消费已成功，也就是说不会从ProcessQueue中移除；
                //但不影响该消息的后续消息的消费，该消息的后续消息仍然能够被消费，但是ProcessQueue的消费位置，即滑动门不会向前移动了；
                //因为只要该消息一直消费遇到异常，那就意味着该消息所对应的queueOffset不能被认为已消费；
                //而我们发送到broker的是当前最小的已被成功消费的queueOffset，所以broker上记录的当前queue的消费位置（消费进度）不会往前移动，
                //直到当前失败的消息消费成功为止。所以，如果我们重启了消费者服务器，那下一次开始消费的消费位置还是从当前失败的位置开始，
                //即便当前失败的消息的后续消息之前已经被消费过了；所以应用需要对每个消息的消费都要支持幂等，不过enode对所有的command和event的处理都支持幂等；
                //以后，我们会在broker上支持重试队列，然后我们可以将消费失败的消息发回到broker上的重试队列，发回到broker上的重试队列成功后，
                //就可以让当前queue的消费位置往前移动了。
                LogMessageHandlingException(consumingMessage, ex);
                _messageRetryQueue.Add(consumingMessage);
            }
        }
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
                    pullRequest.ProcessQueue.IsDropped = true;
                    PersistOffset(pullRequest);
                    _logger.InfoFormat("Dropped pull request, consumerId:{0}, group:{1}, topic={2}, queueId={3}", Id, GroupName, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId);
                }
            }

            // Check message queues to add.
            foreach (var messageQueue in messageQueues)
            {
                var key = messageQueue.ToString();
                PullRequest pullRequest;
                if (!_pullRequestDict.TryGetValue(key, out pullRequest))
                {
                    var request = new PullRequest(Id, GroupName, messageQueue, -1);
                    if (_pullRequestDict.TryAdd(key, request))
                    {
                        SchedulePullRequest(request);
                        _logger.InfoFormat("Added pull request, consumerId:{0}, group:{1}, topic={2}, queueId={3}", Id, GroupName, request.MessageQueue.Topic, request.MessageQueue.QueueId);
                    }
                }
            }
        }
        private void RemoveHandledMessage(ConsumingMessage consumingMessage)
        {
            ConsumingMessage consumedMessage;
            if (_handlingMessageDict.TryRemove(consumingMessage.Message.MessageOffset, out consumedMessage))
            {
                consumedMessage.ProcessQueue.RemoveMessage(consumedMessage.Message);
            }
        }
        private void LogMessageHandlingException(ConsumingMessage consumingMessage, Exception exception)
        {
            _logger.Error(string.Format(
                "Message handling has exception, message info:[messageOffset={0}, topic={1}, queueId={2}, queueOffset={3}, storedTime={4}, consumerId={5}, group={6}]",
                consumingMessage.Message.MessageOffset,
                consumingMessage.Message.Topic,
                consumingMessage.Message.QueueId,
                consumingMessage.Message.QueueOffset,
                consumingMessage.Message.StoredTime,
                Id,
                GroupName), exception);
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
                var consumedQueueOffset = pullRequest.ProcessQueue.GetConsumedQueueOffset();
                if (consumedQueueOffset >= 0)
                {
                    if (!pullRequest.ProcessQueue.TryUpdatePreviousConsumedQueueOffset(consumedQueueOffset))
                    {
                        return;
                    }

                    var request = new UpdateQueueOffsetRequest(GroupName, pullRequest.MessageQueue, consumedQueueOffset);
                    var remotingRequest = new RemotingRequest((int)RequestCode.UpdateQueueOffsetRequest, _binarySerializer.Serialize(request));
                    _remotingClient.InvokeOneway(remotingRequest);
                    _logger.DebugFormat("Sent queue consume offset to broker. group:{0}, consumerId:{1}, topic:{2}, queueId:{3}, offset:{4}",
                        GroupName,
                        Id,
                        pullRequest.MessageQueue.Topic,
                        pullRequest.MessageQueue.QueueId,
                        consumedQueueOffset);
                }
            }
            catch (Exception ex)
            {
                if (_isBrokerServerConnected)
                {
                    _logger.Error(string.Format("PersistOffset has exception, consumerId:{0}, group:{1}, topic:{2}, queueId:{3}", Id, GroupName, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId), ex);
                }
            }
        }
        private void SendHeartbeat()
        {
            try
            {
                var consumingQueues = _pullRequestDict.Values.ToList().Select(x => string.Format("{0}-{1}", x.MessageQueue.Topic, x.MessageQueue.QueueId)).ToList();
                _remotingClient.InvokeOneway(new RemotingRequest(
                    (int)RequestCode.ConsumerHeartbeat,
                    _binarySerializer.Serialize(new ConsumerData(Id, GroupName, _subscriptionTopics, consumingQueues))));
            }
            catch (Exception ex)
            {
                if (_isBrokerServerConnected)
                {
                    _logger.Error(string.Format("SendHeartbeat remoting request to broker has exception, consumerId:{0}, group:{1}", Id, GroupName), ex);
                }
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
                var topicQueueIdsFromServer = GetTopicQueueIdsFromServer(topic).ToList();
                IList<MessageQueue> currentQueues;
                var topicQueuesOfLocal = _topicQueuesDict.TryGetValue(topic, out currentQueues) ? currentQueues : new List<MessageQueue>();
                var topicQueueIdsOfLocal = topicQueuesOfLocal.Select(x => x.QueueId).ToList();

                if (IsIntCollectionChanged(topicQueueIdsFromServer, topicQueueIdsOfLocal))
                {
                    var messageQueues = new List<MessageQueue>();
                    foreach (var queueId in topicQueueIdsFromServer)
                    {
                        messageQueues.Add(new MessageQueue(topic, queueId));
                    }
                    _topicQueuesDict[topic] = messageQueues;
                    _logger.DebugFormat("Queues of topic changed, consumerId:{0}, group:{1}, topic:{2}, old queueIds:{3}, new queueIds:{4}", Id, GroupName, topic, string.Join(":", topicQueueIdsOfLocal), string.Join(":", topicQueueIdsFromServer));
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
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, Setting.DefaultTimeoutMilliseconds);
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
        private IEnumerable<int> GetTopicQueueIdsFromServer(string topic)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.GetTopicQueueIdsForConsumer, Encoding.UTF8.GetBytes(topic));
            var remotingResponse = _remotingClient.InvokeSync(remotingRequest, Setting.DefaultTimeoutMilliseconds);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var queueIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return queueIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x));
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueIds has exception, consumerId:{0}, group:{1}, topic:{2}, remoting response code:{3}", Id, GroupName, topic, remotingResponse.Code));
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
            _taskIds.Add(_scheduleService.ScheduleTask("Consumer.RetryMessage", RetryMessage, Setting.RetryMessageInterval, Setting.RetryMessageInterval));

            _executePullRequestWorker.Start();
            _handleMessageWorker.Start();
        }
        private void StopBackgroundJobsInternal()
        {
            foreach (var taskId in _taskIds)
            {
                _scheduleService.ShutdownTask(taskId);
            }
            foreach (var pullRequest in _pullRequestDict.Values)
            {
                pullRequest.ProcessQueue.IsDropped = true;
            }

            _executePullRequestWorker.Stop();
            _handleMessageWorker.Stop();

            if (_pullRequestQueue.Count == 0)
            {
                _pullRequestQueue.Add(null);
            }
            if (_consumingMessageQueue.Count == 0)
            {
                _consumingMessageQueue.Add(null);
            }
            if (_messageRetryQueue.Count == 0)
            {
                _messageRetryQueue.Add(null);
            }

            Clear();
        }
        private void Clear()
        {
            _taskIds.Clear();
            _pullRequestDict.Clear();
            _topicQueuesDict.Clear();
        }
        private bool IsIntCollectionChanged(IList<int> first, IList<int> second)
        {
            if (first.Count != second.Count)
            {
                return true;
            }
            for (var index = 0; index < first.Count; index++)
            {
                if (first[index] != second[index])
                {
                    return true;
                }
            }
            return false;
        }
        private static byte[] SerializePullMessageRequest(PullMessageRequest request)
        {
            using (var stream = new MemoryStream())
            {
                PullMessageRequest.WriteToStream(request, stream);
                return stream.ToArray();
            }
        }

        #endregion

        void ISocketClientEventListener.OnConnectionClosed(ITcpConnectionInfo connectionInfo, SocketError socketError)
        {
            _isBrokerServerConnected = false;
            StopBackgroundJobs();
        }
        void ISocketClientEventListener.OnConnectionEstablished(ITcpConnectionInfo connectionInfo)
        {
            _isBrokerServerConnected = true;
            _waitSocketConnectHandle.Set();
            StartBackgroundJobs();
        }
        void ISocketClientEventListener.OnConnectionFailed(ITcpConnectionInfo connectionInfo, SocketError socketError)
        {
        }
    }
}
