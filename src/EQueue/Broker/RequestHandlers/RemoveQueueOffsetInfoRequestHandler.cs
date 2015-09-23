using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Processors
{
    public class RemoveQueueOffsetInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IOffsetStore _offsetStore;

        public RemoveQueueOffsetInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _offsetStore = ObjectContainer.Resolve<IOffsetStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<RemoveQueueOffsetInfoRequest>(remotingRequest.Body);
            _offsetStore.DeleteQueueOffset(request.ConsumerGroup, request.Topic, request.QueueId);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
