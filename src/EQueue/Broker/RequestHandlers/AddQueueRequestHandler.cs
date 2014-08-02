using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class AddQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IMessageService _messageService;

        public AddQueueRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var addQueueRequest = _binarySerializer.Deserialize<AddQueueRequest>(request.Body);
            _messageService.AddQueue(addQueueRequest.Topic);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, new byte[0]);
        }
    }
}
