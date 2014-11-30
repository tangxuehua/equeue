using ECommon.Components;
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
            var total = 0;
            var messages = _messageService.QueryMessages(queryMessageRequest.Topic, queryMessageRequest.QueueId, queryMessageRequest.Code, queryMessageRequest.RoutingKey, queryMessageRequest.PageIndex, queryMessageRequest.PageSize, out total);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, _binarySerializer.Serialize(new QueryMessageResponse(total, messages)));
        }
    }
}
