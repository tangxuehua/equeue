using System.Threading;
using ECommon.IoC;
using ECommon.Logging;
using EQueue.Protocols;
using ECommon.Remoting;
using ECommon.Serializing;

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
            var sendMessageRequest = _binarySerializer.Deserialize<SendMessageRequest>(request.Body);
            var storeResult = _messageService.StoreMessage(sendMessageRequest.Message, sendMessageRequest.Arg);
            var sendMessageResponse = new SendMessageResponse(
                storeResult.MessageOffset,
                new MessageQueue(sendMessageRequest.Message.Topic, storeResult.QueueId),
                storeResult.QueueOffset);
            var responseData = _binarySerializer.Serialize(sendMessageResponse);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, responseData);
        }
    }
}
