using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Utilities;
using EQueue.Broker.Exceptions;
using EQueue.Broker.LongPolling;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers
{
    public class BatchSendMessageRequestHandler : IRequestHandler
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
        private readonly ITpsStatisticService _tpsStatisticService;
        private const string SendMessageFailedText = "Batch send message failed.";

        public BatchSendMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();
            _notifyWhenMessageArrived = _brokerController.Setting.NotifyWhenMessageArrived;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendRTLogger = ObjectContainer.Resolve<ILoggerFactory>().Create("BatchSendRT");
            var messageWriteQueueThreshold = brokerController.Setting.BatchMessageWriteQueueThreshold;
            _bufferQueue = new BufferQueue<StoreContext>("QueueBufferQueue", messageWriteQueueThreshold, OnQueueMessageCompleted, _logger);
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

            var request = BatchMessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var messages = request.Messages;
            if (messages.Count() == 0)
            {
                throw new ArgumentException("Invalid batchSendMessage, message list cannot be blank.");
            }

            var topic = messages.First().Topic;
            var queueId = request.QueueId;
            var queue = _queueStore.GetQueue(topic, queueId);
            if (queue == null)
            {
                throw new QueueNotExistException(topic, queueId);
            }

            _messageStore.BatchStoreMessageAsync(queue, request.Messages, (batchRecord, parameter) =>
            {
                var storeContext = parameter as StoreContext;
                if (batchRecord.Records.Any(x => x.LogPosition < 0 || string.IsNullOrEmpty(x.MessageId)))
                {
                    storeContext.Success = false;
                }
                else
                {
                    foreach (var record in batchRecord.Records)
                    {
                        storeContext.Queue.AddMessage(record.LogPosition, record.Tag);
                    }
                    storeContext.BatchMessageLogRecord = batchRecord;
                    storeContext.Success = true;
                }
                _bufferQueue.EnqueueMessage(storeContext);
            }, new StoreContext
            {
                RequestHandlerContext = context,
                RemotingRequest = remotingRequest,
                Queue = queue,
                BatchSendMessageRequestHandler = this
            }, request.ProducerAddress);

            foreach (var message in request.Messages)
            {
                _tpsStatisticService.AddTopicSendCount(topic, queueId);
            }

            return null;
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
            public BatchMessageLogRecord BatchMessageLogRecord;
            public BatchSendMessageRequestHandler BatchSendMessageRequestHandler;
            public bool Success;

            public void OnComplete()
            {
                if (Success)
                {
                    var recordCount = BatchMessageLogRecord.Records.Count();
                    var messageResults = new List<BatchMessageItemResult>();
                    foreach (var record in BatchMessageLogRecord.Records)
                    {
                        messageResults.Add(new BatchMessageItemResult(record.MessageId, record.Code, record.QueueOffset, record.CreatedTime, record.StoredTime, record.Tag));
                    }
                    var result = new BatchMessageStoreResult(Queue.Topic, Queue.QueueId, messageResults);
                    var data = BatchMessageUtils.EncodeMessageStoreResult(result);
                    var response = RemotingResponseFactory.CreateResponse(RemotingRequest, data);

                    RequestHandlerContext.SendRemotingResponse(response);

                    if (BatchSendMessageRequestHandler._notifyWhenMessageArrived && recordCount > 0)
                    {
                        BatchSendMessageRequestHandler._suspendedPullRequestManager.NotifyNewMessage(Queue.Topic, Queue.QueueId, BatchMessageLogRecord.Records.First().QueueOffset);
                    }

                    if (recordCount > 0)
                    {
                        var lastRecord = BatchMessageLogRecord.Records.Last();
                        BatchSendMessageRequestHandler._brokerController.AddLatestMessage(lastRecord.MessageId, lastRecord.CreatedTime, lastRecord.StoredTime);
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
