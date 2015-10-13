using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Utils;

namespace EQueue.Broker.Processors
{
    public class CreateTopicRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueService;

        public CreateTopicRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueService = ObjectContainer.Resolve<IQueueStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<CreateTopicRequest>(remotingRequest.Body);
            _queueService.CreateTopic(request.Topic, request.InitialQueueCount);
            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
