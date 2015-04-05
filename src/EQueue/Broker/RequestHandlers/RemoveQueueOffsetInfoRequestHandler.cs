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

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<RemoveQueueOffsetInfoRequest>(remotingRequest.Body);
            _offsetManager.RemoveQueueOffset(request.ConsumerGroup, request.Topic, request.QueueId);
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, new byte[1] { 1 });
        }
    }
}
