using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Clients.Consumers
{
    public class PullMessageService
    {
        #region Private Variables

        private readonly object _lockObj = new object();
        private readonly string _clientId;
        private readonly Consumer _consumer;
        private readonly ClientService _clientService;
        private readonly IBinarySerializer _binarySerializer;
        private readonly BlockingCollection<ConsumingMessage> _consumingMessageQueue;
        private readonly BlockingCollection<ConsumingMessage> _messageRetryQueue;
        private readonly BlockingCollection<QueueMessage> _pulledMessageQueue;
        private readonly Worker _consumeMessageWorker;
        private readonly IScheduleService _scheduleService;
        private IMessageHandler _messageHandler;
        private readonly ILogger _logger;

        #endregion

        public PullMessageService(Consumer consumer, ClientService clientService)
        {
            _consumer = consumer;
            _clientService = clientService;
            _clientId = clientService.GetClientId();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            if (consumer.Setting.AutoPull)
            {
                if (consumer.Setting.MessageHandleMode == MessageHandleMode.Sequential)
                {
                    _consumingMessageQueue = new BlockingCollection<ConsumingMessage>();
                    _consumeMessageWorker = new Worker("ConsumeMessage", () => HandleMessage(_consumingMessageQueue.Take()));
                }
                _messageRetryQueue = new BlockingCollection<ConsumingMessage>();
            }
            else
            {
                _pulledMessageQueue = new BlockingCollection<QueueMessage>();
            }
        }

        public void SetMessageHandler(IMessageHandler messageHandler)
        {
            if (messageHandler == null)
            {
                throw new ArgumentNullException("messageHandler");
            }
            _messageHandler = messageHandler;
        }
        public void SchedulePullRequest(PullRequest pullRequest)
        {
            if (_consumer.Setting.AutoPull && _messageHandler == null)
            {
                _logger.Error("Schedule pullRequest is cancelled as the messageHandler is null.");
                return;
            }
            Task.Factory.StartNew(ExecutePullRequest, pullRequest);
        }
        public void Start()
        {
            if (_consumer.Setting.AutoPull)
            {
                if (_messageHandler == null)
                {
                    throw new Exception("Cannot start as no messageHandler was set, please call SetMessageHandler first.");
                }
                if (_consumer.Setting.MessageHandleMode == MessageHandleMode.Sequential)
                {
                    _consumeMessageWorker.Start();
                }
                _scheduleService.StartTask("RetryMessage", RetryMessage, 1000, _consumer.Setting.RetryMessageInterval);
            }
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }
        public void Stop()
        {
            if (_consumer.Setting.AutoPull)
            {
                if (_consumer.Setting.MessageHandleMode == MessageHandleMode.Sequential)
                {
                    _consumeMessageWorker.Stop();
                }
                _scheduleService.StopTask("RetryMessage");
            }
            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }
        public IEnumerable<QueueMessage> PullMessages(int maxCount, int timeoutMilliseconds, CancellationToken cancellation)
        {
            var totalMessages = new List<QueueMessage>();
            var timeoutCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellation, new CancellationTokenSource(timeoutMilliseconds).Token);

            lock (_lockObj)
            {
                //需要继续取消息的条件：
                //1. timeout超时时间未到；
                //2. 取到消息数未到达maxCount；
                //3. 消息池中还有消息，或者虽然没消息，但是获取到的消息数也是零；
                while (!timeoutCancellationTokenSource.IsCancellationRequested
                    && totalMessages.Count < maxCount
                    && (_pulledMessageQueue.Count > 0 || totalMessages.Count == 0))
                {
                    try
                    {
                        totalMessages.Add(_pulledMessageQueue.Take(timeoutCancellationTokenSource.Token));
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }

            return totalMessages;
        }

        private void ExecutePullRequest(object parameter)
        {
            if (_consumer.Stopped) return;

            var pullRequest = parameter as PullRequest;
            if (pullRequest == null) return;

            PullMessage(pullRequest);
        }
        private void PullMessage(PullRequest pullRequest)
        {
            var brokerConnection = _clientService.GetBrokerConnection(pullRequest.MessageQueue.BrokerName);
            if (brokerConnection == null)
            {
                Task.Factory.StartDelayedTask(5 * 1000, () => SchedulePullRequest(pullRequest));
                _logger.ErrorFormat("Pull message failed as the target broker connection not found, pullRequest:{0}", pullRequest);
                return;
            }
            var remotingClient = brokerConnection.RemotingClient;

            try
            {
                if (_consumer.Stopped) return;
                if (pullRequest.IsDropped) return;

                var messageCount = 0;
                var flowControlThreshold = 0;

                if (_consumer.Setting.AutoPull)
                {
                    messageCount = pullRequest.ProcessQueue.GetMessageCount();
                    flowControlThreshold = _consumer.Setting.PullMessageFlowControlThreshold;
                }
                else
                {
                    messageCount = _pulledMessageQueue.Count;
                    flowControlThreshold = _consumer.Setting.ManualPullLocalMessageQueueMaxSize;
                }

                if (messageCount > flowControlThreshold)
                {
                    var milliseconds = FlowControlUtil.CalculateFlowControlTimeMilliseconds(
                        messageCount,
                        flowControlThreshold,
                        _consumer.Setting.PullMessageFlowControlStepPercent,
                        _consumer.Setting.PullMessageFlowControlStepWaitMilliseconds);
                    Task.Factory.StartDelayedTask(milliseconds, () => SchedulePullRequest(pullRequest));
                    return;
                }

                var request = new PullMessageRequest
                {
                    ConsumerId = _clientId,
                    ConsumerGroup = _consumer.GroupName,
                    MessageQueue = pullRequest.MessageQueue,
                    Tags = string.Join("|", pullRequest.Tags),
                    QueueOffset = pullRequest.NextConsumeOffset,
                    PullMessageBatchSize = _consumer.Setting.PullMessageBatchSize,
                    SuspendPullRequestMilliseconds = _consumer.Setting.SuspendPullRequestMilliseconds,
                    ConsumeFromWhere = _consumer.Setting.ConsumeFromWhere
                };
                var data = SerializePullMessageRequest(request);
                var remotingRequest = new RemotingRequest((int)BrokerRequestCode.PullMessage, data);

                pullRequest.PullStartTime = DateTime.Now;
                remotingClient.InvokeAsync(remotingRequest, _consumer.Setting.PullRequestTimeoutMilliseconds).ContinueWith(pullTask =>
                {
                    try
                    {
                        if (_consumer.Stopped) return;
                        if (pullRequest.IsDropped) return;

                        if (pullTask.Exception != null)
                        {
                            _logger.Error(string.Format("Pull message failed, pullRequest:{0}", pullRequest), pullTask.Exception);
                            SchedulePullRequest(pullRequest);
                            return;
                        }

                        ProcessPullResponse(pullRequest, pullTask.Result, pulledMessages =>
                        {
                            var filterMessages = pulledMessages.Where(x => IsQueueMessageMatchTag(x, pullRequest.Tags));
                            var consumingMessages = filterMessages.Select(x => new ConsumingMessage(x, pullRequest)).ToList();

                            if (_consumer.Setting.AutoPull)
                            {
                                pullRequest.ProcessQueue.AddMessages(consumingMessages);
                                foreach (var consumingMessage in consumingMessages)
                                {
                                    if (_consumer.Setting.MessageHandleMode == MessageHandleMode.Sequential)
                                    {
                                        _consumingMessageQueue.Add(consumingMessage);
                                    }
                                    else
                                    {
                                        Task.Factory.StartNew(HandleMessage, consumingMessage);
                                    }
                                }
                            }
                            else
                            {
                                foreach (var consumingMessage in consumingMessages)
                                {
                                    _pulledMessageQueue.Add(consumingMessage.Message);
                                }
                            }
                        });
                    }
                    catch (Exception ex)
                    {
                        if (_consumer.Stopped) return;
                        if (pullRequest.IsDropped) return;
                        if (remotingClient.IsConnected)
                        {
                            string remotingResponseBodyLength;
                            if (pullTask.Result != null)
                            {
                                remotingResponseBodyLength = pullTask.Result.ResponseBody.Length.ToString();
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
                if (_consumer.Stopped) return;
                if (pullRequest.IsDropped) return;

                if (remotingClient.IsConnected)
                {
                    _logger.Error(string.Format("PullMessage has exception, pullRequest:{0}", pullRequest), ex);
                }
                SchedulePullRequest(pullRequest);
            }
        }
        private void ProcessPullResponse(PullRequest pullRequest, RemotingResponse remotingResponse, Action<IEnumerable<QueueMessage>> handlePulledMessageAction)
        {
            if (remotingResponse == null)
            {
                _logger.ErrorFormat("Pull message response is null, pullRequest:{0}", pullRequest);
                SchedulePullRequest(pullRequest);
                return;
            }

            if (remotingResponse.ResponseCode == -1)
            {
                _logger.ErrorFormat("Pull message failed, pullRequest:{0}, errorMsg:{1}", pullRequest, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
                SchedulePullRequest(pullRequest);
                return;
            }

            if (remotingResponse.ResponseCode == (short)PullStatus.Found)
            {
                var messages = DecodeMessages(pullRequest, remotingResponse.ResponseBody);
                if (messages.Count() > 0)
                {
                    handlePulledMessageAction(messages);
                    pullRequest.NextConsumeOffset = messages.Last().QueueOffset + 1;
                }
            }
            else if (remotingResponse.ResponseCode == (short)PullStatus.NextOffsetReset)
            {
                var newOffset = BitConverter.ToInt64(remotingResponse.ResponseBody, 0);
                ResetNextConsumeOffset(pullRequest, newOffset);
            }
            else if (remotingResponse.ResponseCode == (short)PullStatus.NoNewMessage)
            {
                //No new message to consume.
            }
            else if (remotingResponse.ResponseCode == (short)PullStatus.Ignored)
            {
                _logger.InfoFormat("Pull request was ignored, pullRequest:{0}", pullRequest);
                return;
            }
            else if (remotingResponse.ResponseCode == (short)PullStatus.BrokerIsCleaning)
            {
                Thread.Sleep(5000);
            }

            //Schedule the next pull request.
            SchedulePullRequest(pullRequest);
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
            if (_consumer.Stopped) return;
            if (consumingMessage == null) return;
            if (consumingMessage.PullRequest.IsDropped) return;
            if (consumingMessage.IsIgnored)
            {
                RemoveHandledMessage(consumingMessage);
                return;
            }

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
        private void RemoveHandledMessage(ConsumingMessage consumedMessage)
        {
            consumedMessage.PullRequest.ProcessQueue.RemoveMessage(consumedMessage);
        }
        private void LogMessageHandlingException(ConsumingMessage consumingMessage, Exception exception)
        {
            _logger.Error(string.Format(
                "Message handling has exception, message info:[messageId:{0}, topic:{1}, queueId:{2}, queueOffset:{3}, createdTime:{4}, storedTime:{5}, brokerName:{6}, consumerGroup:{7}]",
                consumingMessage.Message.MessageId,
                consumingMessage.Message.Topic,
                consumingMessage.Message.QueueId,
                consumingMessage.Message.QueueOffset,
                consumingMessage.Message.CreatedTime,
                consumingMessage.Message.StoredTime,
                consumingMessage.PullRequest.MessageQueue.BrokerName,
                _consumer.GroupName), exception);
        }
        private void ResetNextConsumeOffset(PullRequest pullRequest, long newOffset)
        {
            var brokerConnection = _clientService.GetBrokerConnection(pullRequest.MessageQueue.BrokerName);
            if (brokerConnection == null)
            {
                _logger.ErrorFormat("Reset nextConsumeOffset failed as broker is unavailable, pullRequest: {0}, newOffset: {1}", pullRequest, newOffset);
                return;
            }
            var remotingClient = brokerConnection.AdminRemotingClient;

            try
            {
                var oldOffset = pullRequest.NextConsumeOffset;
                pullRequest.NextConsumeOffset = newOffset;
                pullRequest.ProcessQueue.MarkAllConsumingMessageIgnored();
                pullRequest.ProcessQueue.Reset();

                var request = new UpdateQueueOffsetRequest(_consumer.GroupName, pullRequest.MessageQueue, newOffset - 1);
                var remotingRequest = new RemotingRequest((int)BrokerRequestCode.UpdateQueueConsumeOffsetRequest, _binarySerializer.Serialize(request));
                remotingClient.InvokeOneway(remotingRequest);
                _logger.InfoFormat("Resetted nextConsumeOffset, [pullRequest:{0}, oldOffset:{1}, newOffset:{2}]", pullRequest, oldOffset, newOffset);
            }
            catch (Exception ex)
            {
                if (remotingClient.IsConnected)
                {
                    _logger.Error(string.Format("Reset nextConsumeOffset failed, pullRequest: {0}, newOffset: {1}", pullRequest, newOffset), ex);
                }
            }
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
                var messageLength = ByteUtil.DecodeInt(buffer, nextOffset, out nextOffset);
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
                    messageLength = ByteUtil.DecodeInt(buffer, nextOffset, out nextOffset);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Decode pull return message has exception, pullRequest: {0}", pullRequest), ex);
            }

            return messages;
        }
        private static byte[] SerializePullMessageRequest(PullMessageRequest request)
        {
            using (var stream = new MemoryStream())
            {
                PullMessageRequest.WriteToStream(request, stream);
                return stream.ToArray();
            }
        }
    }
}
