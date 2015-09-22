using System;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
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
                throw new Exception(string.Format("Queue not exist, topic: {0}, queueId: {1}", message.Topic, queueId));
            }

            //消息写文件需要加锁，确保顺序写文件
            MessageStoreResult result = null;
            lock (_syncObj)
            {
                var queueOffset = queue.CurrentOffset;
                var messagePosition = _messageStore.StoreMessage(queueId, queueOffset, message, request.RoutingKey);
                queue.AddMessage(messagePosition);
                queue.IncrementCurrentOffset();
                var messageId = CreateMessageId(messagePosition);
                result = new MessageStoreResult(message.Key, messageId, message.Code, message.Topic, queueId, queueOffset);
            }

            _suspendedPullRequestManager.NotifyNewMessage(request.Message.Topic, result.QueueId, result.QueueOffset);
            var data = MessageUtils.EncodeMessageStoreResult(result);
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }

        private static string CreateMessageId(long messagePosition)
        {
            //TODO，还要结合当前的Broker的IP作为MessageId的一部分
            return messagePosition.ToString();
        }
    }
}
