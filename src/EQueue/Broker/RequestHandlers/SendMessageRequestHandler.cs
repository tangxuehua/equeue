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
        private SuspendedPullRequestManager _suspendedPullRequestManager;
        private IMessageService _messageService;
        private ILogger _logger;

        public SendMessageRequestHandler()
        {
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = MessageUtils.DecodeSendMessageRequest(remotingRequest.Body);
            var storeResult = _messageService.StoreMessage(request.Message, request.QueueId, request.RoutingKey);
            _suspendedPullRequestManager.NotifyNewMessage(request.Message.Topic, storeResult.MessageLogRecord.QueueId, storeResult.MessageLogRecord.QueueOffset);
            var data = MessageUtils.EncodeMessageSendResponse(new SendMessageResponse(
                request.Message.Key,
                storeResult.MessageLogRecord.MessageId,
                storeResult.MessageLogRecord.MessageOffset,
                request.Message.Code,
                request.Message.Topic,
                request.QueueId,
                storeResult.MessageLogRecord.QueueOffset));
            return RemotingResponseFactory.CreateResponse(remotingRequest, data);
        }
    }
}
