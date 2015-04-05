using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class DisableQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IMessageService _messageService;

        public DisableQueueRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _messageService = ObjectContainer.Resolve<IMessageService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var disableQueueRequest = _binarySerializer.Deserialize<DisableQueueRequest>(remotingRequest.Body);
            _messageService.DisableQueue(disableQueueRequest.Topic, disableQueueRequest.QueueId);
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, new byte[1] { 1 });
        }
    }
}
