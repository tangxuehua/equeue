using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class DeleteQueueRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public DeleteQueueRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<DeleteQueueRequest>(remotingRequest.Body);
            _queueStore.DeleteQueue(request.Topic, request.QueueId);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
