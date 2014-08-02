using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class RemoveQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IMessageService _messageService;

        public RemoveQueueRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var removeQueueRequest = _binarySerializer.Deserialize<RemoveQueueRequest>(request.Body);
            _messageService.RemoveQueue(removeQueueRequest.Topic, removeQueueRequest.QueueId);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, new byte[0]);
        }
    }
}
