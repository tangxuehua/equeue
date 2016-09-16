using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Broker.Exceptions;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Utils;

namespace EQueue.Broker.RequestHandlers.Admin
{
    public class GetTopicQueueInfoRequestHandler : IRequestHandler
    {
        private IBinarySerializer _binarySerializer;
        private IQueueStore _queueStore;
        private IConsumeOffsetStore _offsetStore;

        public GetTopicQueueInfoRequestHandler()
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _offsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            if (BrokerController.Instance.IsCleaning)
            {
                throw new BrokerCleanningException();
            }

            var request = _binarySerializer.Deserialize<GetTopicQueueInfoRequest>(remotingRequest.Body);
            var topicQueueInfoList = _queueStore.GetTopicQueueInfoList(request.Topic);

            return RemotingResponseFactory.CreateResponse(remotingRequest, _binarySerializer.Serialize(topicQueueInfoList));
        }


    }
}
