using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Socketing;
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Clients.Consumers
{
    public class Consumer
    {
        #region Private Members

        private readonly object _lockObject;
        private readonly SocketRemotingClient _remotingClient;
        private readonly SocketRemotingClient _adminRemotingClient;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IDictionary<string, HashSet<string>> _subscriptionTopics;
        private readonly ConcurrentDictionary<string, IList<MessageQueue>> _topicQueuesDict;
        private readonly ConcurrentDictionary<string, PullRequest> _pullRequestDict;
        private readonly BlockingCollection<ConsumingMessage> _messageRetryQueue;
        private readonly IScheduleService _scheduleService;
        private readonly IAllocateMessageQueueStrategy _allocateMessageQueueStragegy;
        private readonly ILogger _logger;
        private readonly BlockingCollection<ConsumingMessage> _consumingMessageQueue;
        private readonly Worker _consumeMessageWorker;
        private IMessageHandler _messageHandler;
        private bool _stoped;

        #endregion

        #region Public Properties

        public ConsumerSetting Setting { get; private set; }
        public string GroupName { get; private set; }
        public IDictionary<string, HashSet<string>> SubscriptionTopics
        {
            get { return _subscriptionTopics; }
        }

        #endregion

        #region Constructors

        public Consumer(string groupName) : this(groupName, new ConsumerSetting()) { }
        public Consumer(string groupName, ConsumerSetting setting)
        {
            if (groupName == null)
            {
                throw new ArgumentNullException("groupName");
            }
            GroupName = groupName;
            Setting = setting ?? new ConsumerSetting();

            _lockObject = new object();
            _subscriptionTopics = new Dictionary<string, HashSet<string>>();
            _topicQueuesDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
            _pullRequestDict = new ConcurrentDictionary<string, PullRequest>();
            _remotingClient = new SocketRemotingClient(Setting.BrokerAddress, Setting.SocketSetting, Setting.LocalAddress);
            _adminRemotingClient = new SocketRemotingClient(Setting.BrokerAdminAddress, Setting.SocketSetting, Setting.LocalAdminAddress);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _allocateMessageQueueStragegy = ObjectContainer.Resolve<IAllocateMessageQueueStrategy>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _adminRemotingClient.RegisterConnectionEventListener(new ConnectionEventListener(this));

            if (Setting.MessageHandleMode == MessageHandleMode.Sequential)
            {
                _consumingMessageQueue = new BlockingCollection<ConsumingMessage>();
                _consumeMessageWorker = new Worker("ConsumeMessage", () => HandleMessage(_consumingMessageQueue.Take()));
            }
            _messageRetryQueue = new BlockingCollection<ConsumingMessage>();
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
            if (Setting.MessageHandleMode == MessageHandleMode.Sequential)
            {
                _consumeMessageWorker.Start();
            }
            _scheduleService.StartTask("RetryMessage", RetryMessage, 1000, Setting.RetryMessageInterval);
            _remotingClient.Start();
            _adminRemotingClient.Start();
            _logger.InfoFormat("Consumer started, group: {0}.", GroupName);
            return this;
        }
        public Consumer Shutdown()
        {
            _stoped = true;
            _remotingClient.Shutdown();
            _adminRemotingClient.Shutdown();
            if (Setting.MessageHandleMode == MessageHandleMode.Sequential)
            {
                _consumeMessageWorker.Stop();
            }
            _scheduleService.StopTask("RetryMessage");
            _logger.Info("Consumer shutdown.");
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
            return this;
        }
        public IEnumerable<MessageQueue> GetCurrentQueues()
        {
            return _pullRequestDict.Values.Select(x => x.MessageQueue);
        }

        #endregion

        #region Private Methods

        private void SchedulePullRequest(PullRequest pullRequest)
        {
            Task.Factory.StartNew(ExecutePullRequest, pullRequest);
        }
        private void ExecutePullRequest(object parameter)
        {
            if (_stoped) return;

            var pullRequest = parameter as PullRequest;
            if (pullRequest == null) return;

            PullMessage(pullRequest);
        }
        private void PullMessage(PullRequest pullRequest)
        {
            try
            {
                if (_stoped) return;
                if (pullRequest.ProcessQueue.IsDropped) return;

                var messageCount = pullRequest.ProcessQueue.GetMessageCount();
                var flowControlThreshold = Setting.PullMessageFlowControlThreshold;

                if (flowControlThreshold > 0 && messageCount >= flowControlThreshold)
                {
                    var milliseconds = FlowControlUtil.CalculateFlowControlTimeMilliseconds(
                        messageCount,
                        flowControlThreshold,
                        Setting.PullMessageFlowControlStepPercent,
                        Setting.PullMessageFlowControlStepWaitMilliseconds);
                    Task.Factory.StartDelayedTask(milliseconds, () => SchedulePullRequest(pullRequest));
                    return;
                }

                var request = new PullMessageRequest
                {
                    ConsumerId = GetConsumerId(),
                    ConsumerGroup = GroupName,
                    MessageQueue = pullRequest.MessageQueue,
                    Tags = string.Join("|", pullRequest.Tags),
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
                        if (_remotingClient.IsConnected)
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

                if (_remotingClient.IsConnected)
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

            if (remotingResponse.Code == (short)PullStatus.Found)
            {
                var messages = DecodeMessages(pullRequest, remotingResponse.Body);
                if (messages.Count() > 0)
                {
                    var filterMessages = messages.Where(x => IsQueueMessageMatchTag(x, pullRequest.Tags));
                    pullRequest.ProcessQueue.AddMessages(filterMessages);
                    foreach (var message in filterMessages)
                    {
                        var consumingMessage = new ConsumingMessage(message, pullRequest.ProcessQueue);
                        if (Setting.MessageHandleMode == MessageHandleMode.Sequential)
                        {
                            _consumingMessageQueue.Add(consumingMessage);
                        }
                        else
                        {
                            Task.Factory.StartNew(HandleMessage, consumingMessage);
                        }
                    }
                    pullRequest.NextConsumeOffset = messages.Last().QueueOffset + 1;
                }
            }
            else if (remotingResponse.Code == (short)PullStatus.NextOffsetReset)
            {
                var newOffset = BitConverter.ToInt64(remotingResponse.Body, 0);
                var oldOffset = pullRequest.NextConsumeOffset;
                pullRequest.NextConsumeOffset = newOffset;
                pullRequest.ProcessQueue.Reset();
                _logger.InfoFormat("Reset queue next consume offset. topic:{0}, queueId:{1}, old offset:{2}, new offset:{3}", pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId, oldOffset, newOffset);
            }
            else if (remotingResponse.Code == (short)PullStatus.NoNewMessage)
            {
                if (_logger.IsDebugEnabled)
                {
                    _logger.DebugFormat("No new message found, pullRequest:{0}", pullRequest);
                }
            }
            else if (remotingResponse.Code == (short)PullStatus.Ignored)
            {
                if (_logger.IsDebugEnabled)
                {
                    _logger.DebugFormat("Pull request was ignored, pullRequest:{0}", pullRequest);
                }
                return;
            }
            else if (remotingResponse.Code == (short)PullStatus.BrokerIsCleaning)
            {
                Thread.Sleep(5000);
            }

            //Schedule the next pull request.
            SchedulePullRequest(pullRequest);
        }
        private bool IsQueueMessageMatchTag(QueueMessage message, HashSet<string> tags)
        {
            if (tags == null || tags.Count == 0)
            {
                return true;
            }
            foreach (var tag in tags)
            {
                if (tag == "*" || tag == message.Tag)
                {
                    return true;
                }
            }
            return false;
        }
        private IEnumerable<QueueMessage> DecodeMessages(PullRequest pullRequest, byte[] buffer)
        {
            var messages = new List<QueueMessage>();
            if (buffer == null || buffer.Length <= 4)
            {
                return messages;
            }

            try
            {
                var nextOffset = 0;
                var messageLength = MessageUtils.DecodeInt(buffer, nextOffset, out nextOffset);
                while (messageLength > 0)
                {
                    var message = new QueueMessage();
                    var messageBytes = new byte[messageLength];
                    Buffer.BlockCopy(buffer, nextOffset, messageBytes, 0, messageLength);
                    nextOffset += messageLength;
                    message.ReadFrom(messageBytes);
                    if (!message.IsValid())
                    {
                        _logger.ErrorFormat("Invalid message, pullRequest: {0}", pullRequest);
                        continue;
                    }
                    messages.Add(message);
                    if (nextOffset >= buffer.Length)
                    {
                        break;
                    }
                    messageLength = MessageUtils.DecodeInt(buffer, nextOffset, out nextOffset);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Decode pull return message has exception, pullRequest: {0}", pullRequest), ex);
            }

            return messages;
        }

        private void RetryMessage()
        {
            ConsumingMessage message;
            if (_messageRetryQueue.TryTake(out message))
            {
                HandleMessage(message);
            }
        }
        private void HandleMessage(object parameter)
        {
            var consumingMessage = parameter as ConsumingMessage;
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
                //放入重试队列后，会定期对该消息进行重试，重试队列中的消息会定时被取出一个来重试。
                //通过这样的设计，可以确保消费有异常的消息不会被认为消费已成功，也就是说不会从ProcessQueue中移除；
                //但不影响该消息的后续消息的消费，该消息的后续消息仍然能够被消费，但是ProcessQueue的消费位置，即滑动门不会向前移动了；
                //因为只要该消息一直消费遇到异常，那就意味着该消息所对应的queueOffset不能被认为已消费；
                //而我们发送到broker的是当前最小的已被成功消费的queueOffset，所以broker上记录的当前queue的消费位置（消费进度）不会往前移动，
                //直到当前失败的消息消费成功为止。所以，如果我们重启了消费者服务器，那下一次开始消费的消费位置还是从当前失败的位置开始，
                //即便当前失败的消息的后续消息之前已经被消费过了；所以应用需要对每个消息的消费都要支持幂等；
                //未来，我们会在broker上支持重试队列，然后我们可以将消费失败的消息发回到broker上的重试队列，发回到broker上的重试队列成功后，
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
                    _logger.Error(string.Format("RebalanceClustering has exception, group: {0}, topic: {1}", GroupName, subscriptionTopic), ex);
                }
            }
        }
        private void RebalanceClustering(KeyValuePair<string, HashSet<string>> subscriptionTopic)
        {
            IList<MessageQueue> messageQueues;
            if (_topicQueuesDict.TryGetValue(subscriptionTopic.Key, out messageQueues))
            {
                var consumerIdList = QueryGroupConsumers(subscriptionTopic.Key).ToList();
                consumerIdList.Sort();

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

                var allocatedMessageQueues = _allocateMessageQueueStragegy.Allocate(GetConsumerId(), messageQueueList, consumerIdList);

                UpdatePullRequestDict(subscriptionTopic, allocatedMessageQueues.ToList());
            }
        }
        private void UpdatePullRequestDict(KeyValuePair<string, HashSet<string>> subscriptionTopic, IList<MessageQueue> messageQueues)
        {
            // Check message queues to remove
            var toRemovePullRequestKeys = new List<string>();
            foreach (var pullRequest in _pullRequestDict.Values.Where(x => x.MessageQueue.Topic == subscriptionTopic.Key))
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
                    _logger.InfoFormat("Dropped pull request, group: {0}, topic: {1}, queueId: {2}", GroupName, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId);
                }
            }

            // Check message queues to add.
            foreach (var messageQueue in messageQueues)
            {
                var key = messageQueue.ToString();
                PullRequest pullRequest;
                if (!_pullRequestDict.TryGetValue(key, out pullRequest))
                {
                    var request = new PullRequest(GetConsumerId(), GroupName, messageQueue, -1, subscriptionTopic.Value);
                    if (_pullRequestDict.TryAdd(key, request))
                    {
                        SchedulePullRequest(request);
                        _logger.InfoFormat("Added pull request, group: {0}, topic: {1}, queueId: {2}, tags: {3}", GroupName, request.MessageQueue.Topic, request.MessageQueue.QueueId, string.Join("|", request.Tags));
                    }
                }
            }
        }
        private void RemoveHandledMessage(ConsumingMessage consumedMessage)
        {
            consumedMessage.ProcessQueue.RemoveMessage(consumedMessage.Message);
        }
        private void LogMessageHandlingException(ConsumingMessage consumingMessage, Exception exception)
        {
            _logger.Error(string.Format(
                "Message handling has exception, message info:[messageId:{0}, topic:{1}, queueId:{2}, queueOffset:{3}, createdTime:{4}, storedTime:{5}, consumerGroup:{6}]",
                consumingMessage.Message.MessageId,
                consumingMessage.Message.Topic,
                consumingMessage.Message.QueueId,
                consumingMessage.Message.QueueOffset,
                consumingMessage.Message.CreatedTime,
                consumingMessage.Message.StoredTime,
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
                    _adminRemotingClient.InvokeOneway(remotingRequest);
                    if (_logger.IsDebugEnabled)
                    {
                        _logger.DebugFormat("Sent queue consume offset to broker. group: {0}, topic: {1}, queueId: {2}, offset: {3}",
                            GroupName,
                            pullRequest.MessageQueue.Topic,
                            pullRequest.MessageQueue.QueueId,
                            consumedQueueOffset);
                    }
                }
            }
            catch (Exception ex)
            {
                if (_adminRemotingClient.IsConnected)
                {
                    _logger.Error(string.Format("PersistOffset has exception, group: {0}, topic: {1}, queueId: {2}", GroupName, pullRequest.MessageQueue.Topic, pullRequest.MessageQueue.QueueId), ex);
                }
            }
        }
        private void SendHeartbeat()
        {
            try
            {
                var consumingQueues = _pullRequestDict.Values.ToList().Select(x => QueueKeyUtil.CreateQueueKey(x.MessageQueue.Topic, x.MessageQueue.QueueId)).ToList();
                _remotingClient.InvokeOneway(new RemotingRequest(
                    (int)RequestCode.ConsumerHeartbeat,
                    _binarySerializer.Serialize(new ConsumerData(GetConsumerId(), GroupName, _subscriptionTopics.Keys, consumingQueues))));
            }
            catch (Exception ex)
            {
                if (_remotingClient.IsConnected)
                {
                    _logger.Error(string.Format("SendHeartbeat remoting request to broker has exception, group: {0}", GroupName), ex);
                }
            }
        }
        private void RefreshTopicQueues()
        {
            foreach (var topic in SubscriptionTopics.Keys)
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
                    if (_logger.IsDebugEnabled)
                    {
                        _logger.DebugFormat("Queues of topic changed, group: {0}, topic: {1}, old queueIds: {2}, new queueIds: {3}", GroupName, topic, string.Join(":", topicQueueIdsOfLocal), string.Join(":", topicQueueIdsFromServer));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("UpdateTopicQueues failed, group: {0}, topic: {1}", GroupName, topic), ex);
            }
        }
        private IEnumerable<string> QueryGroupConsumers(string topic)
        {
            var queryConsumerRequest = _binarySerializer.Serialize(new QueryConsumerRequest(GroupName, topic));
            var remotingRequest = new RemotingRequest((int)RequestCode.QueryGroupConsumer, queryConsumerRequest);
            var remotingResponse = _adminRemotingClient.InvokeSync(remotingRequest, 60000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var consumerIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return consumerIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            }
            else
            {
                throw new Exception(string.Format("QueryGroupConsumers has exception, group: {0}, topic: {1}, remoting response code: {2}", GroupName, topic, remotingResponse.Code));
            }
        }
        private IEnumerable<int> GetTopicQueueIdsFromServer(string topic)
        {
            var remotingRequest = new RemotingRequest((int)RequestCode.GetTopicQueueIdsForConsumer, Encoding.UTF8.GetBytes(topic));
            var remotingResponse = _adminRemotingClient.InvokeSync(remotingRequest, 60000);
            if (remotingResponse.Code == (int)ResponseCode.Success)
            {
                var queueIds = Encoding.UTF8.GetString(remotingResponse.Body);
                return queueIds.Split(new[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x));
            }
            else
            {
                throw new Exception(string.Format("GetTopicQueueIds has exception, group: {0}, topic: {1}, remoting response code: {2}", GroupName, topic, remotingResponse.Code));
            }
        }
        private void StartBackgroundJobs()
        {
            lock (_lockObject)
            {
                _scheduleService.StartTask("RefreshTopicQueues", RefreshTopicQueues, 1000, Setting.UpdateTopicQueueCountInterval);
                _scheduleService.StartTask("SendHeartbeat", SendHeartbeat, 1000, Setting.HeartbeatBrokerInterval);
                _scheduleService.StartTask("Rebalance", Rebalance, 1000, Setting.RebalanceInterval);
                _scheduleService.StartTask("PersistOffset", PersistOffset, 1000, Setting.SendConsumerOffsetInterval);
            }
        }
        private void StopBackgroundJobs()
        {
            lock (_lockObject)
            {
                _scheduleService.StopTask("RefreshTopicQueues");
                _scheduleService.StopTask("SendHeartbeat");
                _scheduleService.StopTask("Rebalance");
                _scheduleService.StopTask("PersistOffset");

                foreach (var pullRequest in _pullRequestDict.Values)
                {
                    pullRequest.ProcessQueue.IsDropped = true;
                }

                _pullRequestDict.Clear();
                _topicQueuesDict.Clear();
            }
        }
        private static bool IsIntCollectionChanged(IList<int> first, IList<int> second)
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
        private string GetConsumerId()
        {
            return ClientIdFactory.CreateClientId(_remotingClient.LocalEndPoint as IPEndPoint);
        }

        #endregion

        class ConnectionEventListener : IConnectionEventListener
        {
            private readonly Consumer _consumer;

            public ConnectionEventListener(Consumer consumer)
            {
                _consumer = consumer;
            }

            public void OnConnectionAccepted(ITcpConnection connection) { }
            public void OnConnectionFailed(SocketError socketError) { }
            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                _consumer.StopBackgroundJobs();
            }
            public void OnConnectionEstablished(ITcpConnection connection)
            {
                _consumer.StartBackgroundJobs();
            }
        }
    }
}
