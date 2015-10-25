using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class DeleteTopicRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;

        public DeleteTopicRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<DeleteTopicRequest>(remotingRequest.Body);
            _queueStore.DeleteTopic(request.Topic);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
