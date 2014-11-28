using ECommon.Components;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryMessageRequestHandler : IRequestHandler
    {
        private IMessageService _messageService;
        private IBinarySerializer _binarySerializer;

        public QueryMessageRequestHandler()
        {
            _messageService = ObjectContainer.Resolve<IMessageService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var queryMessageRequest = _binarySerializer.Deserialize<QueryMessageRequest>(request.Body);
            var messages = _messageService.QueryMessages(queryMessageRequest.Topic, queryMessageRequest.QueueId, queryMessageRequest.Code);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, _binarySerializer.Serialize(new PullMessageResponse(messages)));
        }
    }
}
