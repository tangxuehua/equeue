using System;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using EQueue.Broker.Exceptions;
using EQueue.Broker.LongPolling;
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
        private readonly object _syncObj = new object();
        private readonly BrokerController _brokerController;

        public SendMessageRequestHandler(BrokerController brokerController)
        {
            _brokerController = brokerController;
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
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

            //消息写文件需要加锁，确保顺序写文件
            MessageStoreResult result = null;
            lock (_syncObj)
            {
                var queueOffset = queue.NextOffset;
                var messageRecord = _messageStore.StoreMessage(queueId, queueOffset, message);
                queue.AddMessage(messageRecord.LogPosition, message.Tag);
                queue.IncrementNextOffset();
                result = new MessageStoreResult(messageRecord.MessageId, message.Code, message.Topic, queueId, queueOffset, message.Tag);
            }

            //如果需要立即通知所有消费者有新消息，则立即通知
            if (_brokerController.Setting.NotifyWhenMessageArrived)
            {
                _suspendedPullRequestManager.NotifyNewMessage(request.Message.Topic, result.QueueId, result.QueueOffset);
            }

            var data = MessageUtils.EncodeMessageStoreResult(result);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
