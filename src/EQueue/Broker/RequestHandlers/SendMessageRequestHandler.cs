using ECommon.IoC;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Processors
{
    public class SendMessageRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public SendMessageRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var sendMessageRequest = MessageUtils.DecodeSendMessageRequest(request.Body);
            var storeResult = _messageService.StoreMessage(sendMessageRequest.Message, sendMessageRequest.QueueId);
            var sendMessageResponse = new SendMessageResponse(
                storeResult.MessageOffset,
                new MessageQueue(sendMessageRequest.Message.Topic, storeResult.QueueId),
                storeResult.QueueOffset);
            var responseData = _binarySerializer.Serialize(sendMessageResponse);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, responseData);
        }
    }
}
