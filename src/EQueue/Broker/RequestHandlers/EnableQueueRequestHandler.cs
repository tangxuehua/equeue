using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class EnableQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IMessageService _messageService;

        public EnableQueueRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var enableQueueRequest = _binarySerializer.Deserialize<EnableQueueRequest>(request.Body);
            _messageService.EnableQueue(enableQueueRequest.Topic, enableQueueRequest.QueueId);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, new byte[0]);
        }
    }
}
