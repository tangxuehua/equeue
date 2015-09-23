using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class UpdateQueueOffsetRequestHandler : IRequestHandler
    {
        private IOffsetStore _offsetStore;
        private IBinarySerializer _binarySerializer;

        public UpdateQueueOffsetRequestHandler()
        {
            _offsetStore = ObjectContainer.Resolve<IOffsetStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<UpdateQueueOffsetRequest>(remotingRequest.Body);
            _offsetStore.UpdateQueueOffset(
                request.MessageQueue.Topic,
                request.MessageQueue.QueueId,
                request.QueueOffset,
                request.ConsumerGroup);
            return null;
        }
    }
}
