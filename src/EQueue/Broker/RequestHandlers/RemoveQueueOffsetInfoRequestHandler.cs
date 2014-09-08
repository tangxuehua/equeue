using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class RemoveQueueOffsetInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IOffsetManager _offsetManager;

        public RemoveQueueOffsetInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var removeQueueOffsetInfoRequest = _binarySerializer.Deserialize<RemoveQueueOffsetInfoRequest>(request.Body);
            _offsetManager.RemoveQueueOffset(removeQueueOffsetInfoRequest.ConsumerGroup, removeQueueOffsetInfoRequest.Topic, removeQueueOffsetInfoRequest.QueueId);
            return new RemotingResponse((int)ResponseCode.Success, request.Sequence, new byte[1] { 1 });
        }
    }
}
