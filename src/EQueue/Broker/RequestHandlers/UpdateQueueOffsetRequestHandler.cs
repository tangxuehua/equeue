using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.RequestHandlers
{
    public class UpdateQueueOffsetRequestHandler : IRequestHandler
    {
        private IConsumeOffsetStore _offsetStore;
        private IBinarySerializer _binarySerializer;

        public UpdateQueueOffsetRequestHandler()
        {
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                return null;
            }

            var request = _binarySerializer.Deserialize<UpdateQueueOffsetRequest>(remotingRequest.Body);
            _offsetStore.UpdateConsumeOffset(
                request.MessageQueue.Topic,
                request.MessageQueue.QueueId,
                request.QueueOffset,
                request.ConsumerGroup);
            return null;
        }
    }
}
