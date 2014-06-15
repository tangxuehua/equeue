using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class PullRequest
    {
        #region Private Variables

        private readonly object _lockObject;
        private readonly SocketRemotingClient _remotingClient;
        private readonly Worker _pullMessageWorker;
        private readonly Worker _handleMessageWorker;
        private readonly ILogger _logger;
        private readonly IBinarySerializer _binarySerializer;
        private readonly BlockingCollection<QueueMessage> _messageQueue;
        private readonly BlockingCollection<QueueMessage> _messageRetryQueue;
        private readonly ConcurrentDictionary<long, QueueMessage> _handlingMessageDict;
        private readonly MessageHandleMode _messageHandleMode;
        private readonly IMessageHandler _messageHandler;
        private readonly IScheduleService _scheduleService;
        private readonly PullRequestSetting _setting;
        private int _retryMessageTaskId;
        private long _flowControlTimes;
        private long _queueOffset;
        private bool _stoped;

        #endregion

        #region Public Properties

        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }

        #endregion

        #region Constructors

        public PullRequest(
            string consumerId,
            string groupName,
            MessageQueue messageQueue,
            long queueOffset,
            SocketRemotingClient remotingClient,
            MessageHandleMode messageHandleMode,
            IMessageHandler messageHandler,
            PullRequestSetting setting)
        {
            ConsumerId = consumerId;
            GroupName = groupName;
            MessageQueue = messageQueue;
            ProcessQueue = new ProcessQueue();

            _lockObject = new object();
            _queueOffset = queueOffset;
            _remotingClient = remotingClient;
            _setting = setting;
            _messageHandleMode = messageHandleMode;
            _messageHandler = messageHandler;
            _messageQueue = new BlockingCollection<QueueMessage>(new ConcurrentQueue<QueueMessage>());
            _messageRetryQueue = new BlockingCollection<QueueMessage>(new ConcurrentQueue<QueueMessage>());
            _handlingMessageDict = new ConcurrentDictionary<long, QueueMessage>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _pullMessageWorker = new Worker("PullRequest.PullMessage", PullMessage);
            _handleMessageWorker = new Worker("PullRequest.HandleMessage", HandleMessage);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        #endregion

        public void Start()
        {
            StartPullMessageWorker();
            StartHandleMessageWorker();
            StartRetryMessageTask();
        }
        public void Stop()
        {
            StopPullMessageWorker();
            StopHandleMessageWorker();
            StopRetryMessageTask();
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId={0}, Group={1}, MessageQueue={2}, QueueOffset={3}, Stoped={4}]", ConsumerId, GroupName, MessageQueue, _queueOffset, _stoped);
        }

        private void PullMessage()
        {
            try
            {
                if (_stoped) return;

                var messageCount = ProcessQueue.GetMessageCount();

                if (messageCount >= _setting.PullThresholdForQueue)
                {
                    Thread.Sleep(_setting.PullTimeDelayMillsWhenFlowControl);
                    if ((_flowControlTimes++ % 1000) == 0)
                    {
                        _logger.WarnFormat("Detect that the message local process queue has too many messages, so do flow control. pullRequest={0}, queueMessageCount={1}, flowControlTimes={2}", this, messageCount, _flowControlTimes);
                    }
                }

                var request = new PullMessageRequest
                {
                    ConsumerGroup = GroupName,
                    MessageQueue = MessageQueue,
                    QueueOffset = _queueOffset,
                    PullMessageBatchSize = _setting.PullMessageBatchSize
                };
                var data = _binarySerializer.Serialize(request);
                var remotingRequest = new RemotingRequest((int)RequestCode.PullMessage, data);
                var remotingResponse = _remotingClient.InvokeSync(remotingRequest, _setting.PullRequestTimeoutMilliseconds);

                if (_stoped) return;

                var response = _binarySerializer.Deserialize<PullMessageResponse>(remotingResponse.Body);

                if (remotingResponse.Code == (int)PullStatus.Found && response.Messages.Count() > 0)
                {
                    _queueOffset += response.Messages.Count();
                    ProcessQueue.AddMessages(response.Messages);
                    response.Messages.ForEach(queueMessage => _messageQueue.Add(queueMessage));
                }
                else if (remotingResponse.Code == (int)PullStatus.NextOffsetReset && response.NextOffset != null)
                {
                    _queueOffset = response.NextOffset.Value;
                }
            }
            catch (Exception ex)
            {
                if (!_stoped)
                {
                    _logger.Error(string.Format("PullMessage has exception, pullRequest:{0}", this), ex);
                }
            }
        }
        private void HandleMessage()
        {
            var queueMessage = _messageQueue.Take();

            if (_stoped) return;
            if (queueMessage == null) return;

            var handleAction = new Action(() =>
            {
                if (!_handlingMessageDict.TryAdd(queueMessage.MessageOffset, queueMessage))
                {
                    _logger.DebugFormat("Ignore to handle message [messageOffset={0}, topic={1}, queueId={2}, queueOffset={3}, consumerId={4}, group={5}], as it is being handling.",
                        queueMessage.MessageOffset,
                        queueMessage.Topic,
                        queueMessage.QueueId,
                        queueMessage.QueueOffset,
                        ConsumerId,
                        GroupName);
                    return;
                }
                HandleMessage(queueMessage);
            });

            if (_messageHandleMode == MessageHandleMode.Sequential)
            {
                handleAction();
            }
            else if (_messageHandleMode == MessageHandleMode.Parallel)
            {
                Task.Factory.StartNew(handleAction);
            }
        }
        private void RetryMessage()
        {
            HandleMessage(_messageRetryQueue.Take());
        }
        private void HandleMessage(QueueMessage queueMessage)
        {
            if (_stoped) return;
            if (queueMessage == null) return;

            try
            {
                _messageHandler.Handle(queueMessage, new MessageContext(currentQueueMessage => RemoveHandledMessage(currentQueueMessage.MessageOffset)));
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
                LogMessageHandlingException(queueMessage, ex);
                _messageRetryQueue.Add(queueMessage);
            }
        }
        private void RemoveHandledMessage(long messageOffset)
        {
            QueueMessage queueMessage;
            if (_handlingMessageDict.TryRemove(messageOffset, out queueMessage))
            {
                ProcessQueue.RemoveMessage(queueMessage);
            }
        }
        private void LogMessageHandlingException(QueueMessage queueMessage, Exception exception)
        {
            _logger.Error(string.Format(
                "Message handling has exception, message info:[messageOffset={0}, topic={1}, queueId={2}, queueOffset={3}, storedTime={4}, consumerId={5}, group={6}]",
                queueMessage.MessageOffset,
                queueMessage.Topic,
                queueMessage.QueueId,
                queueMessage.QueueOffset,
                queueMessage.StoredTime,
                ConsumerId,
                GroupName), exception);
        }

        private void StartPullMessageWorker()
        {
            _stoped = false;
            _pullMessageWorker.Start();
        }
        private void StopPullMessageWorker()
        {
            _stoped = true;
            _pullMessageWorker.Stop();
        }
        private void StartHandleMessageWorker()
        {
            _handleMessageWorker.Start();
        }
        private void StopHandleMessageWorker()
        {
            _handleMessageWorker.Stop();
            if (_messageQueue.Count == 0)
            {
                _messageQueue.Add(null);
            }
        }
        private void StartRetryMessageTask()
        {
            lock (_lockObject)
            {
                if (_retryMessageTaskId == 0)
                {
                    _retryMessageTaskId = _scheduleService.ScheduleTask("PullRequest.RetryMessage", RetryMessage, _setting.RetryMessageInterval, _setting.RetryMessageInterval);
                }
            }
        }
        private void StopRetryMessageTask()
        {
            lock (_lockObject)
            {
                if (_retryMessageTaskId > 0)
                {
                    _scheduleService.ShutdownTask(_retryMessageTaskId);
                    if (_messageRetryQueue.Count == 0)
                    {
                        _messageRetryQueue.Add(null);
                    }
                    _retryMessageTaskId = 0;
                }
            }
        }
    }
}
