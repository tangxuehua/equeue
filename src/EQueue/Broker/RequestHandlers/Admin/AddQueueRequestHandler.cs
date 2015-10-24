using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class AddQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public AddQueueRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var addQueueRequest = _binarySerializer.Deserialize<AddQueueRequest>(remotingRequest.Body);
            _queueStore.AddQueue(addQueueRequest.Topic);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
