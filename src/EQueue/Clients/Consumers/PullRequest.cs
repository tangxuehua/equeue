using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Extensions;
using ECommon.IoC;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Clients.Consumers
{
    public class PullRequest
    {
        private readonly SocketRemotingClient _remotingClient;
        private readonly Worker _pullMessageWorker;
        private readonly Worker _handleMessageWorker;
        private readonly ILogger _logger;
        private readonly IBinarySerializer _binarySerializer;
        private readonly BlockingCollection<WrappedMessage> _messageQueue;
        private readonly ConcurrentDictionary<long, WrappedMessage> _handlingMessageDict;
        private readonly MessageHandleMode _messageHandleMode;
        private readonly IMessageHandler _messageHandler;
        private readonly PullRequestSetting _setting;
        private long _flowControlTimes;
        private long _queueOffset;
        private bool _stoped;

        public string ConsumerId { get; private set; }
        public string GroupName { get; private set; }
        public MessageQueue MessageQueue { get; private set; }
        public ProcessQueue ProcessQueue { get; private set; }

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

            _queueOffset = queueOffset;
            _remotingClient = remotingClient;
            _setting = setting;
            _messageHandleMode = messageHandleMode;
            _messageHandler = messageHandler;
            _messageQueue = new BlockingCollection<WrappedMessage>(new ConcurrentQueue<WrappedMessage>());
            _handlingMessageDict = new ConcurrentDictionary<long, WrappedMessage>();
            _pullMessageWorker = new Worker(() =>
            {
                try
                {
                    PullMessage();
                }
                catch (Exception ex)
                {
                    if (!_stoped)
                    {
                        _logger.Error(string.Format("[{0}]: PullMessage has unknown exception. PullRequest: {1}.", ConsumerId, this), ex);
                    }
                }
            });
            _handleMessageWorker = new Worker(HandleMessage);
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        #endregion

        public void Start()
        {
            _pullMessageWorker.Start();
            _handleMessageWorker.Start();
        }

        public void Stop()
        {
            _pullMessageWorker.Stop();
            _handleMessageWorker.Stop();
            _stoped = true;
        }

        public override string ToString()
        {
            return string.Format("[ConsumerId={0}, GroupName={1}, MessageQueue={2}, QueueOffset={3}, Stoped={4}]", ConsumerId, GroupName, MessageQueue, _queueOffset, _stoped);
        }

        private void PullMessage()
        {
            var messageCount = ProcessQueue.GetMessageCount();

            if (messageCount >= _setting.PullThresholdForQueue)
            {
                Thread.Sleep(_setting.PullTimeDelayMillsWhenFlowControl);
                if ((_flowControlTimes++ % 3000) == 0)
                {
                    _logger.WarnFormat("[{0}]: the consumer message buffer is full, so do flow control, [messageCount={1},pullRequest={2},flowControlTimes={3}]", ConsumerId, messageCount, this, _flowControlTimes);
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
            var remotingResponse = default(RemotingResponse);

            try
            {
                remotingResponse = _remotingClient.InvokeSync(remotingRequest, _setting.PullRequestTimeoutMilliseconds);
            }
            catch (Exception ex)
            {
                if (!_stoped)
                {
                    _logger.Error(string.Format("[{0}]: PullMessage has exception. RemotingRequest: {1}, PullRequest: {2}.", ConsumerId, remotingRequest, this), ex);
                }
                return;
            }

            if (_stoped)
            {
                return;
            }

            var response = _binarySerializer.Deserialize<PullMessageResponse>(remotingResponse.Body);

            if (remotingResponse.Code == (int)PullStatus.Found && response.Messages.Count() > 0)
            {
                _queueOffset += response.Messages.Count();
                ProcessQueue.AddMessages(response.Messages);
                response.Messages.ForEach(x => _messageQueue.Add(new WrappedMessage(MessageQueue, x, ProcessQueue)));
            }
            else if (remotingResponse.Code == (int)PullStatus.NextOffsetReset && response.NextOffset != null)
            {
                _queueOffset = response.NextOffset.Value;
            }
        }
        private void HandleMessage()
        {
            var wrappedMessage = _messageQueue.Take();
            Action handleAction = () =>
            {
                if (_stoped)
                {
                    return;
                }
                if (!_handlingMessageDict.TryAdd(wrappedMessage.QueueMessage.MessageOffset, wrappedMessage))
                {
                    _logger.DebugFormat("Ignore to handle message [offset={0}, topic={1}, queueId={2}, queueOffset={3}, consumerId={4}], as it is being handling.",
                        wrappedMessage.QueueMessage.MessageOffset,
                        wrappedMessage.QueueMessage.Topic,
                        wrappedMessage.QueueMessage.QueueId,
                        wrappedMessage.QueueMessage.QueueOffset,
                        ConsumerId);
                    return;
                }
                try
                {
                    _messageHandler.Handle(wrappedMessage.QueueMessage, new MessageContext(queueMessage => RemoveMessage(queueMessage.MessageOffset)));
                }
                catch (Exception ex)
                {
                    //TODO，目前，对于消费失败（遇到异常）的消息，我们仅仅记录错误日志，然后仍将该消息移除，即让消费位置（滑动门）可以往前移动；
                    //以后，这里需要将消费失败的消息发回到Broker上的重试队列进行重试。
                    _logger.Error("Handle message has exception. Currently, we still take this message as consumed.", ex);
                    RemoveMessage(wrappedMessage.QueueMessage.MessageOffset);
                }
            };
            if (_messageHandleMode == MessageHandleMode.Sequential)
            {
                handleAction();
            }
            else if (_messageHandleMode == MessageHandleMode.Parallel)
            {
                Task.Factory.StartNew(handleAction);
            }
        }

        private void RemoveMessage(long messageOffset)
        {
            WrappedMessage wrappedMessage;
            if (_handlingMessageDict.TryRemove(messageOffset, out wrappedMessage))
            {
                wrappedMessage.ProcessQueue.RemoveMessage(wrappedMessage.QueueMessage);
            }
        }
    }
}
