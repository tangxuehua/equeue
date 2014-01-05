using EQueue.Infrastructure;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;
using EQueue.Remoting.Requests;
using EQueue.Remoting.Responses;

namespace EQueue.Broker.Processors
{
    public class SendMessageRequestProcessor : IRequestProcessor
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public SendMessageRequestProcessor()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse ProcessRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var sendMessageRequest = _binarySerializer.Deserialize<SendMessageRequest>(request.Body);
            var storeResult = _messageService.StoreMessage(sendMessageRequest.Message, sendMessageRequest.Arg);
            var sendMessageResponse = new SendMessageResponse(
                storeResult.MessageId,
                storeResult.MessageOffset,
                new MessageQueue(sendMessageRequest.Message.Topic, storeResult.QueueId),
                storeResult.QueueOffset);
            var responseData = _binarySerializer.Serialize(sendMessageResponse);
            _logger.Debug(sendMessageResponse);
            var remotingResponse = new RemotingResponse((int)ResponseCode.Success, responseData);
            remotingResponse.Sequence = request.Sequence;
            return remotingResponse;
        }
    }
}
