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

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<RemoveQueueRequest>(remotingRequest.Body);
            _messageService.RemoveQueue(request.Topic, request.QueueId);
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, new byte[1] { 1 });
        }
    }
}
