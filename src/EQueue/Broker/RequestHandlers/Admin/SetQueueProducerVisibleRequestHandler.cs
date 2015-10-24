using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class SetQueueProducerVisibleRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public SetQueueProducerVisibleRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<SetQueueProducerVisibleRequest>(remotingRequest.Body);
            _queueStore.SetProducerVisible(request.Topic, request.QueueId, request.Visible);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
