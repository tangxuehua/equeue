using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;

namespace EQueue.Broker.Processors
{
    public class CreateTopicRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueService _queueService;

        public CreateTopicRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueService = ObjectContainer.Resolve<IQueueService>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<CreateTopicRequest>(remotingRequest.Body);
            _queueService.CreateTopic(request.Topic, request.InitialQueueCount);
            return new RemotingResponse((int)ResponseCode.Success, remotingRequest.Sequence, new byte[1] { 1 });
        }
    }
}
