using System.Linq;
using EQueue.Infrastructure;
using EQueue.Infrastructure.IoC;
using EQueue.Infrastructure.Logging;
using EQueue.Protocols;
using EQueue.Remoting;
using EQueue.Remoting.Requests;
using EQueue.Remoting.Responses;

namespace EQueue.Broker.Processors
{
    public class PullMessageRequestProcessor : IRequestProcessor
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;
        private ILogger _logger;

        public PullMessageRequestProcessor()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);
        }

        public RemotingResponse ProcessRequest(IRequestHandlerContext requestContext, RemotingRequest request)
        {
            var pullMessageRequest = _binarySerializer.Deserialize<PullMessageRequest>(request.Body);
            var messages = _messageService.GetMessages(
                pullMessageRequest.MessageQueue.Topic,
                pullMessageRequest.MessageQueue.QueueId,
                pullMessageRequest.QueueOffset,
                pullMessageRequest.PullMessageBatchSize);
            var pullMessageResponse = new PullMessageResponse(messages);
            var responseData = _binarySerializer.Serialize(pullMessageResponse);
            var remotingResponse = new RemotingResponse(messages.Count() > 0 ? (int)PullStatus.Found : (int)PullStatus.NoNewMessage, responseData);
            remotingResponse.Sequence = request.Sequence;
            return remotingResponse;
        }
    }
}
