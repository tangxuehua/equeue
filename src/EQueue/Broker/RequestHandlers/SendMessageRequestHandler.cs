using System;
using System.Text;
using System.Threading;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Utilities;
using EQueue.Broker.Exceptions;
using EQueue.Broker.LongPolling;
using EQueue.Broker.Storage;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers
{
    public class SendMessageRequestHandler : IRequestHandler
    {
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly IMessageStore _messageStore;
        private readonly IQueueStore _queueStore;
        private readonly ILogger _logger;
        private readonly ILogger _sendRTLogger;
        private readonly object _syncObj = new object();
        private readonly BrokerController _brokerController;
        private readonly bool _notifyWhenMessageArrived;
        private readonly BufferQueue<StoreContext> _bufferQueue;
        private const string SendMessageFailedText = "Send message failed.";
        private readonly IScheduleService _scheduleService;
        private long _totalCount0;
        private long _totalCount1;
        private long _totalCount2;
        private long _rt0TotalTime;
        private double _rt1TotalTime;
        private double _rt2TotalTime;
        private double _rt3TotalTime;

        public SendMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _notifyWhenMessageArrived = _brokerController.Setting.NotifyWhenMessageArrived;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendRTLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("SendRT");
            var messageWriteQueueThreshold = brokerController.Setting.MessageWriteQueueThreshold;
            _bufferQueue = new BufferQueue<StoreContext>("QueueBufferQueue", messageWriteQueueThreshold, OnQueueMessageCompleted, _logger);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
        }

        public void Start()
        {
            _scheduleService.StartTask("PrintSendRT", PrintSendRT, 1000, 1000);
        }
        public void Shutdown()
        {
            _scheduleService.StopTask("PrintSendRT");
        }
        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (remotingRequest.Body.Length > _brokerController.Setting.MessageMaxSize)
            {
                throw new Exception("Message size cannot exceed max message size:" + _brokerController.Setting.MessageMaxSize);
            }

            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = MessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var message = request.Message;
            var queueId = request.QueueId;
            var queue = _queueStore.GetQueue(message.Topic, queueId);
            if (queue == null)
            {
                throw new QueueNotExistException(message.Topic, queueId);
            }

            var t0 = (long)((DateTime.Now - message.CreatedTime).TotalMilliseconds * 1000);
            Interlocked.Increment(ref _totalCount0);
            Interlocked.Add(ref _rt0TotalTime, t0);

            _messageStore.StoreMessageAsync(queue, message, (record, parameter) =>
            {
                _totalCount1++;
                var current = DateTime.Now;
                var t1 = (current - record.CreatedTime).TotalMilliseconds;
                var t2 = (current - record.StoredTime).TotalMilliseconds;
                _rt1TotalTime += t1;
                _rt2TotalTime += t2;
                var storeContext = parameter as StoreContext;
                storeContext.Queue.AddMessage(record.LogPosition, record.Tag);
                storeContext.MessageLogRecord = record;
                _bufferQueue.EnqueueMessage(storeContext);
            }, new StoreContext
            {
                RequestHandlerContext = context,
                RemotingRequest = remotingRequest,
                Queue = queue,
                SendMessageRequestHandler = this
            });

            return null;
        }

        private void PrintSendRT()
        {
            if (_sendRTLogger != null)
            {
                if (_totalCount0 > 0 && _totalCount1 > 0 && _totalCount2 > 0)
                {
                    _sendRTLogger.DebugFormat("rt0: {0}, rt1: {1}, rt2: {2}, rt3: {3}",
                        (double)_rt0TotalTime / _totalCount0 / 1000,
                        _rt1TotalTime / _totalCount1,
                        _rt2TotalTime / _totalCount1,
                        _rt3TotalTime / _totalCount2);
                }
            }
        }
        private void OnQueueMessageCompleted(StoreContext storeContext)
        {
            storeContext.OnComplete();
        }
        class StoreContext
        {
            public IRequestHandlerContext RequestHandlerContext;
            public RemotingRequest RemotingRequest;
            public Queue Queue;
            public MessageLogRecord MessageLogRecord;
            public SendMessageRequestHandler SendMessageRequestHandler;

            public void OnComplete()
            {
                if (MessageLogRecord.LogPosition >= 0 && !string.IsNullOrEmpty(MessageLogRecord.MessageId))
                {
                    var result = new MessageStoreResult(
                        MessageLogRecord.MessageId,
                        MessageLogRecord.Code,
                        MessageLogRecord.Topic,
                        MessageLogRecord.QueueId,
                        MessageLogRecord.QueueOffset,
                        MessageLogRecord.CreatedTime,
                        MessageLogRecord.StoredTime,
                        MessageLogRecord.Tag);
                    var data = MessageUtils.EncodeMessageStoreResult(result);
                    var response = RemotingResponseFactory.CreateResponse(RemotingRequest, data);

                    RequestHandlerContext.SendRemotingResponse(response);

                    SendMessageRequestHandler._totalCount2++;
                    SendMessageRequestHandler._rt3TotalTime += (DateTime.Now - result.CreatedTime).TotalMilliseconds;

                    if (SendMessageRequestHandler._notifyWhenMessageArrived)
                    {
                        SendMessageRequestHandler._suspendedPullRequestManager.NotifyNewMessage(MessageLogRecord.Topic, result.QueueId, result.QueueOffset);
                    }
                }
                else
                {
                    var response = RemotingResponseFactory.CreateResponse(RemotingRequest, ResponseCode.Failed, Encoding.UTF8.GetBytes(SendMessageFailedText));
                    RequestHandlerContext.SendRemotingResponse(response);
                }
            }
        }
    }
}
