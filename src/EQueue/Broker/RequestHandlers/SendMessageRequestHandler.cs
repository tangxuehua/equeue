using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using EQueue.Broker.Exceptions;
using EQueue.Broker.LongPolling;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Processors
{
    public class SendMessageRequestHandler : IRequestHandler
    {
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly IMessageStore _messageStore;
        private readonly IQueueStore _queueService;
        private readonly ILogger _logger;
        private readonly object _syncObj = new object();

        public SendMessageRequestHandler()
        {
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _queueService = ObjectContainer.Resolve<IQueueStore>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = MessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var message = request.Message;
            var queueId = request.QueueId;
            var queue = _queueService.GetQueue(message.Topic, queueId);
            if (queue == null)
            {
                throw new QueueNotExistException(message.Topic, queueId);
            }

            //消息写文件需要加锁，确保顺序写文件
            MessageStoreResult result = null;
            lock (_syncObj)
            {
                var queueOffset = queue.NextOffset;
                var messageRecord = _messageStore.StoreMessage(queueId, queueOffset, message, request.RoutingKey);
                queue.AddMessage(messageRecord.LogPosition);
                queue.IncrementNextOffset();
                result = new MessageStoreResult(message.Key, messageRecord.MessageId, message.Code, message.Topic, queueId, queueOffset);
            }

            //如果需要立即通知所有消费者有新消息，则立即通知
            if (BrokerController.Instance.Setting.NotifyWhenMessageArrived)
            {
                _suspendedPullRequestManager.NotifyNewMessage(request.Message.Topic, result.QueueId, result.QueueOffset);
            }

            var data = MessageUtils.EncodeMessageStoreResult(result);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
