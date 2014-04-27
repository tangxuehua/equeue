using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class UpdateQueueOffsetRequestHandler : IRequestHandler
    {
        private IOffsetManager _offsetManager;
        private IBinarySerializer _binarySerializer;

        public UpdateQueueOffsetRequestHandler()
        {
            _offsetManager = ObjectContainer.Resolve<IOffsetManager>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest request)
        {
            var updateQueueOffsetRequest = _binarySerializer.Deserialize<UpdateQueueOffsetRequest>(request.Body);
            _offsetManager.UpdateQueueOffset(
                updateQueueOffsetRequest.MessageQueue.Topic,
                updateQueueOffsetRequest.MessageQueue.QueueId,
                updateQueueOffsetRequest.QueueOffset,
                updateQueueOffsetRequest.ConsumerGroup);
            return null;
        }
    }
}
