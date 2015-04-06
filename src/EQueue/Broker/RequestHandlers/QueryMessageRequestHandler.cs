using System.Linq;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class QueryMessageRequestHandler : IRequestHandler
    {
        private readonly IMessageStore _messageStore;
        private readonly IBinarySerializer _binarySerializer;

        public QueryMessageRequestHandler()
        {
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<QueryMessageRequest>(remotingRequest.Body);
            var total = 0;
            var messages = _messageStore.QueryMessages(request.Topic, request.QueueId, request.Code, request.RoutingKey, request.PageIndex, request.PageSize, out total).ToList();
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, _binarySerializer.Serialize(new QueryMessageResponse(total, messages)));
        }
    }
}
